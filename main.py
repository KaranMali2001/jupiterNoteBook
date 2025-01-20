import threading
import logging
import json
import uuid
import os
import time
from typing import Dict, Any
from queue import Empty
from dockerSession import DockerSessionManager
import docker
import requests
import websockets
from websockets.exceptions import WebSocketException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




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

async def handle_kernel_messages(kernel_ws, client_ws: WebSocket, msg_id: str):
    """Handle messages from the kernel and forward execution results to the client"""
    try:
        execution_count = None
        has_received_response = False
        
        while True:
            logger.info("Waiting for message from kernel...")
            response = await kernel_ws.recv()
            kernel_message = json.loads(response)
            logger.info(f"Message received: {kernel_message}")
            
            # Get the parent message ID
            parent_id = kernel_message.get("parent_header", {}).get("msg_id")
            
            # Only process messages that are responses to our execute request
            if parent_id == msg_id:
                msg_type = kernel_message["header"]["msg_type"]
                
                if msg_type in ["execute_result", "stream", "error", "status"]:
                    logger.info(f"Processing message type: {msg_type}")
                    
                    if msg_type == "execute_result":
                        has_received_response = True
                        output_data = {
                            "type": "output",
                            "content": kernel_message["content"]["data"]["text/plain"]
                        }
                        await client_ws.send_json(output_data)
                        
                    elif msg_type == "stream":
                        has_received_response = True
                        output_data = {
                            "type": "output",
                            "content": kernel_message["content"]["text"]
                        }
                        await client_ws.send_json(output_data)
                        
                    elif msg_type == "error":
                        has_received_response = True
                        output_data = {
                            "type": "error",
                            "content": "\n".join(kernel_message["content"]["traceback"])
                        }
                        await client_ws.send_json(output_data)
                        
                    elif msg_type == "status":
                        state = kernel_message["content"]["execution_state"]
                        output_data = {
                            "type": "status",
                            "content": state
                        }
                        await client_ws.send_json(output_data)
                        
                        # If we've received a response and we're now idle, we're done
                        if state == "idle" and has_received_response:
                            logger.info("Execution complete, breaking from kernel message loop")
                            return
                        
                        # If we get idle without any response, wait a bit longer
                        # for potential delayed output
                        elif state == "idle":
                            await asyncio.sleep(0.1)  # Short delay to catch any pending messages
                            if not has_received_response:
                                logger.info("No output received but kernel is idle")
                                return

    except websockets.exceptions.ConnectionClosed:
        logger.info("Kernel WebSocket connection closed.")
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
            logger.info("Waiting for message from client...")
            data = await websocket.receive_text()
            message = json.loads(data)
            logger.info(f"Received message from client: {message}")
            
            if message["type"] == "execute":
                # Generate a unique message ID
                msg_id = str(uuid.uuid4())
                
                # Create Jupyter message
                execute_request = {
                    "header": {
                        "msg_id": msg_id,
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
                logger.info("Sent execute request to kernel")
                await handle_kernel_messages(kernel_ws, websocket, msg_id)
                logger.info("Finished handling kernel messages")

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
        docker_manager.close_session(session_id)
        await websocket.close()
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)