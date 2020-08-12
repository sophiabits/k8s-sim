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
                # Update EndPoints when they are no longer valid
                # Restart failed pods

                for endpoint in self.apiServer.GetEndPoints():
                    print('[NodeController]', endpoint.deploymentLabel, endpoint.pod.status)
                    if endpoint.pod.status == 'FAILED':
                        # Restart this pod (note: api name could be a lot better)
                        # TODO -- does the pod need to stay on the same node? probably.
                        self.apiServer.RemoveEndPoint(endpoint)

                        # Allow pod to be rescheduled by the scheduler
                        endpoint.pod.status = 'PENDING'
                        self.apiServer.etcd.pendingPodList.append(endpoint.pod)

                    # Do we need to handle status == 'TERMINATING'?

                    # if not self.apiServer.CheckEndPoint(endpoint):
                    #     # Pod is not alive, so restart it -- set statuus to PENDING and move to pendingPodList
                    #     # TODO -- does restarting mean we set status = 'PENDING' and move it to pendingPodList here?
                    #     #      -- or does restarting just mean we let DepController handle it in its next loop (which is what I've done for now?)
                    #     pod = endpoint.pod
                    #     print('[NodeController] Restarting pod', pod.podName)
                    #     self.apiServer.etcd.endPointList.remove(endpoint)
                    #     self.apiServer.etcd.runningPodList.remove(pod)

            time.sleep(self.time)
        print("NodeContShutdown")
