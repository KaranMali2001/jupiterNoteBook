import threading
import logging
import json
import uuid
import os
import time
from typing import Dict, Any
from queue import Empty
import logging
import docker
import requests
import websockets
from websockets.exceptions import WebSocketException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DockerSessionManager:
    def __init__ (self,image:str="jupyter/base-notebook"):
        try:
            os.environ["DOCKER_CONFIG"] = "/dev/null"
            self.client=docker.from_env()
            logger.info("Docker started")
            self.client.ping()
            self.image= image
            self.session:Dict[str,Dict[str,Any]]= {}
            self.lock =threading.Lock()
            self._ensure_image()
        except docker.errors.DockerException as e:
            print("error while init is",e)
            raise
    def _ensure_image(self)->None:
        try:
            self.client.images.pull(self.image , auth_config=None)
        except docker.errors.ImageLoadError:
            print("Error while ensuring the image")
            raise
    def create_session(self,session_id:str)-> str:
        with self.lock:
            if session_id in self.session:
                print("session Exist",session_id)
                return self.get_session(session_id)
            try:
                container=self.client.containers.run(
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
                    ports={"8888/tcp":None},
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
                for _ in range(30):
                    container.reload()
                    if container.status=='running':
                        time.sleep(2)
                        break
                else:
                        raise Exception("Did not start in Time")
                port = container.attrs['NetworkSettings']['Ports']['8888/tcp'][0]['HostPort']
                logger.info("getting port from container")
                kernal_url=f"http://localhost:{port}"
                for _ in range(10):
                    try:
                        res= requests.get(f"{kernal_url}/api/status")
                        if res.status_code==200:
                            break
                    except Exception:
                        print("some error while checking api status of kernal line 68")
                else:
                    raise Exception("failed to connect to server")
                self.session[session_id]={
                    "container":container,
                    "kernel_url":kernal_url
                }
                print("Session created with with kernal url",kernal_url)
                return kernal_url
            except Exception as e:
                print("Error while creatinng session ",e)
    def close_session(self,session_id:str)->None:
        with self.lock:
            if session_id not in self.session:
                print("Session does not exist to close")
                return
            try:
                container=self.session[session_id]["container"]
                container.stop(timeout=5)
                container.remove()
                logger.info(f"Docker REmoveddddd")
                del self.session[session_id]
            except Exception as e:
                print("error while closing session",e)
    def __del__(self):
        """Cleanup method to ensure all containers are stopped when the manager is destroyed"""
        try:
            for session_id in list(self.sessions.keys()):
                self.close_session(session_id)
        except Exception:
            pass
    def get_session(self,session_id:str)->str:
        with self.lock:
            if session_id not in self.session:
                raise KeyError("session id does not exist",session_id)
            return self.session[session_id]['kernel_url']