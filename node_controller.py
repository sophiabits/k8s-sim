import threading
import time

from api_server import APIServer

# NodeController is a control loop that monitors the status of WorkerNode objects in the cluster and ensures that the EndPoint objects stored in etcd are up to date.
# The NodeController will remove stale EndPoints and update to show changes in others
class NodeController:
    apiServer: APIServer

    def __init__(self, APISERVER, LOOPTIME):
        self.apiServer = APISERVER
        self.running = True
        self.time = LOOPTIME

    def __call__(self):
        print("NodeController start")
        while self.running:
            with self.apiServer.etcdLock:
                # Check information on each WorkerNode in etcd
                # Update EndPoints when they are no longer valid -- TODO what does that mean?

                # Restart failed pods
                for endpoint in self.apiServer.GetEndPoints():
                    print('[NodeController]', endpoint.pod.podName, endpoint.pod.status)
                    if endpoint.pod.status == 'FAILED':
                        # Restart this pod by marking it as PENDING
                        print(f'[NodeController] Marking failed pod as pending for rescheduling {endpoint.pod.podName}')
                        endpoint.pod.status = 'PENDING'
                        self.apiServer.etcd.runningPodList.remove(endpoint.pod)
                        self.apiServer.etcd.pendingPodList.append(endpoint.pod)

            time.sleep(self.time)
        print("NodeContShutdown")
