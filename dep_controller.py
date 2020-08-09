import threading
import time

from api_server import APIServer

# DepController is a control loop that creates and terminates Pod objects based on
# the expected number of replicas.
class DepController:
    def __init__(self, APISERVER, LOOPTIME):
        self.apiServer = APISERVER
        self.running = True
        self.time = LOOPTIME

    def __call__(self):
        print("depController start")
        while self.running:
            with self.apiServer.etcdLock:
                pass

            time.sleep(self.time)
        print("DepContShutdown")
