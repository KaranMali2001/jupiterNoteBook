import threading
import logging
import json
import uuid
import os
import time
from typing import Dict, Any
from queue import Empty

import docker
import requests
import websockets
from websockets.exceptions import WebSocketException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DockerSessionManager:
    def __init__(self, image: str = "jupyter/base-notebook", mem_limit: str = "512m", cpu_shares: int = 512):
        try:
            # Create an environment with empty config to bypass credential store
            os.environ["DOCKER_CONFIG"] = "/dev/null"
            self.client = docker.from_env()
            self.client.ping()  # Test connection

            self.image = image
            self.mem_limit = mem_limit
            self.cpu_shares = cpu_shares
            self.sessions: Dict[str, Dict[str, Any]] = {}
            self.lock = threading.Lock()
            self._ensure_image()
        except docker.errors.DockerException as e:
            logger.error(f"Failed to connect to Docker daemon: {str(e)}")
            raise

    def _ensure_image(self) -> None:
        try:
            logger.info(f"Checking for Docker image: {self.image}")
            try:
                self.client.images.get(self.image)
                logger.info(f"Image {self.image} already exists locally.")
            except docker.errors.ImageNotFound:
                logger.info(f"Pulling Docker image: {self.image}...")
                self.client.images.pull(self.image, auth_config=None)
                logger.info(f"Image {self.image} pulled successfully.")
        except docker.errors.APIError as e:
            logger.error(f"Error handling image {self.image}: {str(e)}")
            raise

    def create_session(self, session_id: str) -> str:
        with self.lock:
            if session_id in self.sessions:
                logger.info(f"Session {session_id} already exists.")
                return self.get_session(session_id)

            try:
                container = self.client.containers.run(
                    image=self.image,
                    command=[
                        "start-notebook.sh",
                        "--NotebookApp.token=''",
                        "--NotebookApp.password=''",
                        "--NotebookApp.allow_origin='*'",
                        "--NotebookApp.disable_check_xsrf=True",
                        "--NotebookApp.base_url='/'",
                        "--NotebookApp.allow_root=True",
                        "--NotebookApp.authenticate_prometheus=False",
                    ],
                    detach=True,
                    mem_limit=self.mem_limit,
                    cpu_shares=self.cpu_shares,
                    ports={"8888/tcp": None},
                    environment={
                        "JUPYTER_ENABLE_LAB": "yes",
                        "JUPYTER_TOKEN": "",
                        "JUPYTER_PASSWORD": "",
                        "GRANT_SUDO": "yes",
                        "JUPYTER_ALLOW_INSECURE_WRITES": "true",
                        "NB_UID": "1000",
                        "NB_GID": "100",
                    },
                )

                # Wait for container to be ready
                for _ in range(30):
                    container.reload()
                    if container.status == "running":
                        time.sleep(2)  # Give Jupyter server time to start
                        break
                    time.sleep(1)
                else:
                    raise Exception("Container failed to start in time")

                # Get the assigned port
                port = container.attrs["NetworkSettings"]["Ports"]["8888/tcp"][0]["HostPort"]
                kernel_url = f"http://localhost:{port}"

                # Verify the connection is working
                for _ in range(20):
                    try:
                        response = requests.get(f"{kernel_url}/api/status", timeout=1)
                        if response.status_code == 200:
                            time.sleep(2)
                            break
                    except Exception:
                        time.sleep(2)
                else:
                    raise Exception("Failed to connect to Jupyter server")

                self.sessions[session_id] = {
                    "container": container,
                    "kernel_url": kernel_url,
                }

                logger.info(f"Session {session_id} created with kernel URL {kernel_url}")
                return kernel_url

            except Exception as e:
                logger.error(f"Error creating session {session_id}: {str(e)}")
                if "container" in locals():
                    try:
                        container.remove(force=True)
                    except Exception:
                        pass
                raise

    def close_session(self, session_id: str) -> None:
        with self.lock:
            if session_id not in self.sessions:
                logger.warning(f"Session {session_id} does not exist.")
                return

            try:
                container = self.sessions[session_id]["container"]
                container.stop(timeout=5)
                container.remove()
                del self.sessions[session_id]
                logger.info(f"Session {session_id} closed and container removed.")
            except Exception as e:
                logger.error(f"Error closing session {session_id}: {str(e)}")
                raise

    def get_session(self, session_id: str) -> str:
        with self.lock:
            if session_id not in self.sessions:
                raise KeyError(f"Session {session_id} does not exist.")
            return self.sessions[session_id]["kernel_url"]

    def __del__(self):
        """Cleanup method to ensure all containers are stopped when the manager is destroyed"""
        try:
            for session_id in list(self.sessions.keys()):
                self.close_session(session_id)
        except Exception:
            pass

# Initialize FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Docker manager
docker_manager = DockerSessionManager()

async def handle_kernel_messages(kernel_ws, client_ws: WebSocket):
    """Handle messages from the kernel and forward only execution results to the client"""
    try:
        msg_id = None  # Track the current message ID
        while True:
            response = await kernel_ws.recv()
            kernel_message = json.loads(response)
            
            # Get the parent message ID if it exists
            parent_id = kernel_message.get("parent_header", {}).get("msg_id")
            
            msg_type = kernel_message["header"]["msg_type"]
            
            # Only process messages that are responses to our execute request
            if msg_type in ["execute_result", "stream", "error", "status"]:
                # For execute_result, only send the data
                if msg_type == "execute_result":
                    output_data = {
                        "type": "output",
                        "content": kernel_message["content"]["data"]["text/plain"]
                    }
                    await client_ws.send_json(output_data)
                
                # For stream output (stdout/stderr)
                elif msg_type == "stream":
                    output_data = {
                        "type": "output",
                        "content": kernel_message["content"]["text"]
                    }
                    await client_ws.send_json(output_data)
                
                # For errors
                elif msg_type == "error":
                    output_data = {
                        "type": "error",
                        "content": "\n".join(kernel_message["content"]["traceback"])
                    }
                    await client_ws.send_json(output_data)
                
                # For status updates
                elif msg_type == "status":
                    output_data = {
                        "type": "status",
                        "content": kernel_message["content"]["execution_state"]
                    }
                    await client_ws.send_json(output_data)
                    
                    # Only break if this status message belongs to our execute request
                    if (parent_id == msg_id and 
                        kernel_message["content"]["execution_state"] == "idle"):
                        break

    except Exception as e:
        logger.error(f"Error handling kernel messages: {str(e)}")
        await client_ws.send_json({
            "type": "error",
            "message": "Error processing kernel output",
            "details": str(e)
        })
@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    kernel_ws = None
    
    try:
        # Create a new Docker session
        kernel_url = docker_manager.create_session(session_id)
        if not kernel_url.endswith("/"):
            kernel_url += "/"

        # Create a new kernel
        kernel_response = requests.post(f"{kernel_url}api/kernels")
        kernel_response.raise_for_status()
        kernel_id = kernel_response.json()["id"]

        # Convert HTTP URL to WebSocket URL
        ws_url = f"ws://localhost:{kernel_url.split(':')[-1].strip('/')}/api/kernels/{kernel_id}/channels"
        kernel_ws = await websockets.connect(ws_url)
        logger.info(f"Connected to kernel WebSocket: {ws_url}")

        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            print("Received message:", message)
            if message["type"] == "execute":
                # Create Jupyter message
                execute_request = {
                    "header": {
                        "msg_id": str(uuid.uuid4()),
                        "username": "user",
                        "session": session_id,
                        "msg_type": "execute_request",
                        "version": "5.0",
                    },
                    "parent_header": {},
                    "metadata": {},
                    "content": {
                        "code": message["code"],
                        "silent": False,
                        "store_history": True,
                        "user_expressions": {},
                        "allow_stdin": False,
                        "stop_on_error": True,
                    },
                    "channel": "shell",
                    "buffers": [],
                }

                await kernel_ws.send(json.dumps(execute_request))
                print("Sent execute request to kernel")
                await handle_kernel_messages(kernel_ws, websocket)

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket for session {session_id}: {str(e)}")
        if websocket.client_state.CONNECTED:
            await websocket.send_json({
                "type": "error",
                "message": "An error occurred",
                "details": str(e)
            })
    finally:
        if kernel_ws:
            await kernel_ws.close()
            logger.info(f"WebSocket connection closed for session {session_id}")
        
        try:
            docker_manager.close_session(session_id)
            logger.info(f"Container for session {session_id} closed and removed.")
        except Exception as e:
            logger.error(f"Error closing session {session_id}: {str(e)}")
        
        await websocket.close()
        logger.info(f"WebSocket closed for session {session_id}")
@app.get("/health")
async def health_check():
    return {"status": "healthy"}