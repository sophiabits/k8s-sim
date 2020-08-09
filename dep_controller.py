import threading
import time

from api_server import APIServer

# DepController is a control loop that creates and terminates Pod objects based on
# the expected number of replicas.
class DepController:
    apiServer: APIServer

    def __init__(self, APISERVER, LOOPTIME):
        self.apiServer = APISERVER
        self.running = True
        self.time = LOOPTIME

    def __call__(self):
        print("depController start")
        while self.running:
            with self.apiServer.etcdLock:
                for deployment in self.apiServer.GetDeployments():
                    pods_to_create = deployment.expectedReplicas - deployment.currentReplicas

                    for _ in range(0, pods_to_create):
                        # Create pods as needed in order to reach expectedReplicas
                        self.apiServer.CreatePod(deployment)

                    # TODO -- delete 'TERMINATING' pods?

            time.sleep(self.time)
        print("DepContShutdown")
