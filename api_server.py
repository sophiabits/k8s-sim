import threading
from typing import List
import uuid

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

    # GetDeployments method returns the list of deployments stored in etcd
    def GetDeployments(self):
        return self.etcd.deploymentList

    # GetWorkers method returns the list of WorkerNodes stored in etcd
    def GetWorkers(self):
        return self.etcd.nodeList

    # GetPending method returns the list of PendingPods stored in etcd
    def GetPending(self):
        return self.etcd.pendingPodList

    # GetEndPoints method returns the list of EndPoints stored in etcd
    def GetEndPoints(self):
        return self.etcd.endPointList

    # CreateWorker creates a WorkerNode from a list of arguments and adds it to the etcd nodeList
    def CreateWorker(self, info):
        node = WorkerNode(info)
        self.etcd.nodeList.append(node)

    # CreateDeployment creates a Deployment object from a list of arguments and adds it to the etcd deploymentList
    def CreateDeployment(self, info):
        deployment = Deployment(info)
        self.etcd.deploymentList.append(deployment)

    # RemoveDeployment deletes the associated Deployment object from etcd and sets the status of all associated pods to 'TERMINATING'
    def RemoveDeployment(self, deploymentLabel: str):
        endpoints = self.GetEndPointsByLabel(deploymentLabel)

        print('[APIServer] Deleting deployment', deploymentLabel)
        for endpoint in endpoints:
            # Mark the pod as TERMINATING

            print('[APIServer] .. flagging pod as TERMINATING', endpoint.pod.podName)
            self.TerminatePod(endpoint)

        for deployment in self.etcd.deploymentList:
            if deployment.deploymentLabel == deploymentLabel:
                self.etcd.deploymentList.remove(deployment)
                break

    # CreateEndpoint creates an EndPoint object using information from a provided Pod and Node and appends it
    # to the endPointList in etcd
    def CreateEndPoint(self, pod: Pod, worker: WorkerNode):
        endpoint = EndPoint(pod, pod.deploymentLabel, worker)

        assert worker.available_cpu >= pod.assigned_cpu
        worker.available_cpu -= pod.assigned_cpu

        self.etcd.endPointList.append(endpoint)

        # Transfer the pod pendingPodList -> runningPodList
        self.etcd.pendingPodList.remove(pod)
        self.etcd.runningPodList.append(pod)

        # Transition pod state to RUNNING
        pod.status = 'RUNNING'

    # Removes the endpoint and its associated pod from the cluster
    def RemoveEndPoint(self, endpoint: EndPoint):
        endpoint.node.available_cpu += endpoint.pod.assigned_cpu
        self.etcd.endPointList.remove(endpoint)

        # Remove pod from runningPodList
        self.etcd.runningPodList.remove(endpoint.pod)

        for deployment in self.etcd.deploymentList:
            if deployment.deploymentLabel == endpoint.deploymentLabel:
                deployment.currentReplicas -= 1
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

        pod = Pod(f'{deployment.deploymentLabel}:{id_suffix}', deployment.cpuCost, deployment.deploymentLabel)
        print('[APIServer] Created pod:', pod.podName)
        self.etcd.pendingPodList.append(pod)
        deployment.currentReplicas += 1

    # # GetPod returns the pod object stored in the internal podList of a WorkerNode
    # def GetPod(self, endPoint):
    #     pass

    # TerminatePod finds the pod associated with a given EndPoint and sets it's status to 'TERMINATING'
    # No new requests will be sent to a pod marked 'TERMINATING'. Once its current requests have been handled,
    # it will be deleted by the Kubelet
    def TerminatePod(self, endpoint: EndPoint):
        endpoint.pod.status = 'TERMINATING'
        self.etcd.endPointList.remove(endpoint)

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

                print('[APIServer] Crashing pod', pod.podName)
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
