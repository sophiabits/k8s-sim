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
                    # First: Delete any pods which need to be terminated
                    for endpoint in self.apiServer.GetEndPointsByLabel(deployment.deploymentLabel):
                        pod = endpoint.pod
                        if pod.status == 'TERMINATING' and not pod.is_running():
                            # Delete this pod and its endpoint
                            print('[DepController] Deleting TERMINATING pod which has drained', pod.podName)
                            self.apiServer.RemoveEndPoint(endpoint)

                    # Now we can attempt to create or delete pods in order to reach expectedReplicas

                    if deployment.currentReplicas < deployment.expectedReplicas:
                        pods_needed = deployment.expectedReplicas - deployment.currentReplicas
                        for _ in range(0, pods_needed):
                            self.apiServer.CreatePod(deployment)
                    elif deployment.currentReplicas > deployment.expectedReplicas:
                        endpoints = self.apiServer.GetEndPointsByLabel(deployment.deploymentLabel)
                        for endpoint in endpoints:
                            self.apiServer.TerminatePod(endpoint)

                    # Special case: expected and current replicas are 0 -- deployment needs to be deleted
                    if deployment.currentReplicas == 0 and deployment.expectedReplicas == 0:
                        print('[DepController] Deleting deployment', deployment.deploymentLabel)
                        self.apiServer.etcd.deploymentList.remove(deployment)

            time.sleep(self.time)
        print("DepContShutdown")
