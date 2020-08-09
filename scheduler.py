import threading
import time

from api_server import APIServer

# The Scheduler is a control loop that checks for any pods that have been created
# but not yet deployed, found in the etcd pendingPodList.
# It transfers Pod objects from the pendingPodList to the runningPodList and creates an EndPoint object to store in the etcd EndPoint list
# If no WorkerNode is available that can take the pod, it remains in the pendingPodList
class Scheduler(threading.Thread):
    apiServer: APIServer

    def __init__(self, APISERVER, LOOPTIME):
        self.apiServer = APISERVER
        self.running = True
        self.time = LOOPTIME

    def __call__(self):
        print("Scheduler start")
        while self.running:
            with self.apiServer.etcdLock:
                for pod in self.apiServer.GetPending():
                    assert pod.status == 'PENDING'

                    # Try to find a suitable node for this pod
                    for node in self.apiServer.GetWorkers():
                        if node.available_cpu < pod.available_cpu:
                            # Not enough cpu on this node for this pod
                            continue

                        # We found a suitable node
                        print(f'[Scheduler] Assigning pod {pod.podName} to worker {node.label}')
                        self.apiServer.CreateEndPoint(pod, node)

                        # Only need to assign the pod once
                        break

            time.sleep(self.time)
        print("SchedShutdown")
