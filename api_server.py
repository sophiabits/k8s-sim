import threading
from typing import List
import uuid

import metrics

from deployment import Deployment
from end_point import EndPoint
from etcd import Etcd
from pod import Pod
from worker_node import WorkerNode

# The APIServer handles the communication between controllers and the cluster. It houses
# the methods that can be called for cluster management

class APIServer:
    def __init__(self):
        self.etcd = Etcd()
        self.etcdLock = threading.Lock()
        self.requestWaiting = threading.Event()

    def GetDeployments(self) -> List[Deployment]:
        ''' Returns the list of deployments stored in etcd. '''
        return [*self.etcd.deploymentList]

    def GetWorkers(self) -> List[WorkerNode]:
        ''' Returns the list of WorkerNodes stored in etcd. '''
        return [*self.etcd.nodeList]

    def GetPending(self) -> List[Pod]:
        ''' Returns the list of pending pods stored in etcd. '''
        return [*self.etcd.pendingPodList]

    def GetEndPoints(self) -> List[EndPoint]:
        ''' Returns the list of endpoints stored in etcd. '''
        return [*self.etcd.endPointList]

    # CreateWorker creates a WorkerNode from a list of arguments and adds it to the etcd nodeList
    def CreateWorker(self, info):
        node = WorkerNode(info)
        self.etcd.nodeList.append(node)

    # CreateDeployment creates a Deployment object from a list of arguments and adds it to the etcd deploymentList
    def CreateDeployment(self, info):
        deployment = Deployment(info)
        self.etcd.deploymentList.append(deployment)

        metrics.deployment_created(deployment)

    # RemoveDeployment deletes the associated Deployment object from etcd and sets the status of all associated pods to 'TERMINATING'
    def RemoveDeployment(self, deploymentLabel: str):
        # endpoints = self.GetEndPointsByLabel(deploymentLabel)

        # print('[APIServer] Deleting deployment', deploymentLabel)
        # for endpoint in endpoints:
        #     # Mark the pod as TERMINATING

        #     print('[APIServer] .. flagging pod as TERMINATING', endpoint.pod.podName)
        #     self.TerminatePod(endpoint)

        # TODO
        print('[APIServer] Marking deployment for termination', deploymentLabel)

        for deployment in self.etcd.deploymentList:
            if deployment.deploymentLabel == deploymentLabel:
                # XXX: Spec says this method should remove the deployment from etcd,
                #      but on Moodle Stephen has recommended implementing this by setting
                #      expectedReplicas to 0, and then having the DepController manage
                #      deletion of the associated pods.

                # XXX: Stephen has indicated it could be a good idea to set expectedReplicas to 0
                #      here and then just leave deletion of pods to DepController. The issue with
                #      that approach is that the Scheduler could run before DepController, resulting
                #      in a request being routed to a Pod which is going to get shut down on the
                #      next invocation of DepController. Does that matter? Probably not... but it
                # deployment.expectedReplicas = 0

                self.etcd.deploymentList.remove(deployment)
                break
        else:
            print('[APIServer] .. but could not find it!')

    # CreateEndpoint creates an EndPoint object using information from a provided Pod and Node and appends it
    # to the endPointList in etcd
    def CreateEndPoint(self, pod: Pod, worker: WorkerNode):
        endpoint = EndPoint(pod, pod.deploymentLabel, worker)

        assert worker.available_cpu >= pod.assigned_cpu
        worker.available_cpu -= pod.assigned_cpu
        metrics.node_cpu(worker)

        self.etcd.endPointList.append(endpoint)
        self.StartPod(pod)

    # Removes the endpoint and its associated pod from the cluster
    def RemoveEndPoint(self, endpoint: EndPoint):
        assert not endpoint.pod.is_running()

        endpoint.node.available_cpu += endpoint.pod.assigned_cpu
        self.etcd.endPointList.remove(endpoint)
        metrics.node_cpu(endpoint.node)

        # Remove pod from runningPodList
        self.etcd.runningPodList.remove(endpoint.pod)

        for deployment in self.etcd.deploymentList:
            if deployment.deploymentLabel == endpoint.deploymentLabel:
                deployment.currentReplicas -= 1
                # TODO record pod got moved from running pod list?
                metrics.deployment_replicas(deployment)
                break
        else:
            print('[APIServer] Failed to find deployment associated with endpoint!', endpoint)

    # CheckEndPoint checks that the associated pod is still present on the expected WorkerNode
    def CheckEndPoint(self, endpoint: EndPoint) -> bool:
        return endpoint.pod.status == 'RUNNING'

    # GetEndPointsByLabel returns a list of EndPoints associated with a given deployment
    def GetEndPointsByLabel(self, deploymentLabel: str) -> List[EndPoint]:
        return list(filter(
            lambda endpoint: endpoint.deploymentLabel == deploymentLabel,
            self.etcd.endPointList,
        ))

    # CreatePod finds the resource allocations associated with a deployment and creates a pod using those metrics
    def CreatePod(self, deployment: Deployment):
        # TODO document that we changed deploymentLabel -> deployment

        id_suffix = str(uuid.uuid4()).split('-')[0]

        # TODO -- do we adjust currentReplicas now? or only when a pod transitions status from PENDING -> RUNNING?
        assert deployment.currentReplicas < deployment.expectedReplicas

        pod = Pod(f'{deployment.deploymentLabel}/{id_suffix}', deployment.cpuCost, deployment.deploymentLabel)
        # print('[APIServer] Created pod:', pod)
        self.etcd.pendingPodList.append(pod)
        deployment.currentReplicas += 1

        metrics.pod_created(pod)
        metrics.deployment_replicas(deployment)

    def StartPod(self, pod: Pod):
        assert pod.status == 'PENDING'

        self.etcd.pendingPodList.remove(pod)
        self.etcd.runningPodList.append(pod)
        pod.crash.clear()
        pod.status = 'RUNNING'

        metrics.pod_started(pod)

    # # GetPod returns the pod object stored in the internal podList of a WorkerNode
    # def GetPod(self, endPoint):
    #     pass

    # TerminatePod finds the pod associated with a given EndPoint and sets it's status to 'TERMINATING'
    # No new requests will be sent to a pod marked 'TERMINATING'. Once its current requests have been handled,
    # it will be deleted by the Kubelet
    def TerminatePod(self, endpoint: EndPoint):
        endpoint.pod.status = 'TERMINATING'

    # CrashPod finds a pod from a given deployment and sets its status to 'FAILED'
    # Any resource utilisation on the pod will be reset to the base 0
    def CrashPod(self, deploymentLabel: str):
        endpoints = self.GetEndPointsByLabel(deploymentLabel)
        if not endpoints:
            print('[APIServer] Failed to find pod for deployment to crash:', deploymentLabel)
            return

        # Crash the first runnind pod
        for endpoint in endpoints:
            if endpoint.pod.status == 'RUNNING':
                pod = endpoint.pod

                # NOTE: Should probably call pool.shutdown and then make a new
                #       ThreadPoolExecutor when the pod gets rescheduled. But then
                #       we lose the "request crashed" message, so we need to track
                #       pending request ids and... it's too much work rn

                metrics.pod_crashed(pod)
                pod.status = 'FAILED'
                pod.crash.set()
                break
        else:
            print('[APIServer] Failed to find a suitable pod to crash!', deploymentLabel)

    # AssignNode takes a pod in the pendingPodList and transfers it to the internal podList of a specified WorkerNode
    def AssignNode(self, pod, worker):
        pass

    # PushReq adds the incoming request to the handling queue
    def PushReq(self, info):
        self.etcd.reqCreator.submit(self.reqHandle, info)

    # Creates requests and notifies the handler of request to be dealt with
    def reqHandle(self, info):
        # Note: Stephen has indicated reqHandle should make use of Request objects
        # created by etcd.
        self.etcd.pendingReqs.append(info)
        self.requestWaiting.set()
