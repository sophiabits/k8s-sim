import threading
import time

from api_server import APIServer
import metrics

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
            self.work()
            time.sleep(self.time)
        print("DepContShutdown")

    def work(self):
        with self.apiServer.etcdLock:
            for deployment in self.apiServer.GetDeployments():
                # First: Delete any pods which need to be terminated
                for endpoint in self.apiServer.GetEndPointsByLabel(deployment.deploymentLabel):
                    pod = endpoint.pod
                    if pod.status == 'TERMINATING' and not pod.is_running():
                        # Delete this pod and its endpoint
                        # print('[DepController] Deleting TERMINATING pod which has drained', pod)
                        self.apiServer.RemoveEndPoint(endpoint)

                # Now we can attempt to create or delete pods in order to reach expectedReplicas

                # Calculate the 'eventual' replica count of this deployment
                endpoints = self.apiServer.GetEndPointsByLabel(deployment.deploymentLabel)
                terminating_endpoint_count = len(list(filter( # why tf can you not len() a filter object, so stupid
                    lambda endpoint: endpoint.pod.status == 'TERMINATING',
                    endpoints,
                )))
                eventual_replicas = deployment.currentReplicas - terminating_endpoint_count

                if eventual_replicas < deployment.expectedReplicas:
                    pods_needed = deployment.expectedReplicas - deployment.currentReplicas
                    for _ in range(0, pods_needed):
                        self.apiServer.CreatePod(deployment)
                elif eventual_replicas > deployment.expectedReplicas:
                    pods_to_terminate = eventual_replicas - deployment.expectedReplicas
                    pods_terminated = 0
                    print('DepController going to terminate', pods_to_terminate, 'replicas')
                    for endpoint in endpoints:
                        if endpoint.pod.status == 'TERMINATING': continue
                        pods_terminated += 1
                        self.apiServer.TerminatePod(endpoint)

                        if pods_terminated == pods_to_terminate:
                            break
                    else:
                        print('DepController failed to terminate the necessary number of pods!!')

                # Special case: expected and current replicas are 0 -- deployment needs to be deleted
                if deployment.currentReplicas == 0 and deployment.expectedReplicas == 0:
                    self.apiServer.etcd.deploymentList.remove(deployment)
                    metrics.deployment_deleted(deployment)
