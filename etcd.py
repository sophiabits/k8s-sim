from typing import List

from deployment import Deployment
from end_point import EndPoint
from pod import Pod
from request import Request
from worker_node import WorkerNode

class Etcd:
    '''Etcd is the storage component of the cluster that allows for comparison between expected configuration and real time information.'''

    deploymentList: List[Deployment]
    endPointList: List[EndPoint]
    # List of the nodes within the cluster
    nodeList: List[WorkerNode]
    # List of pods created by the deployment controller which are waiting to be scheduled
    pendingPodList: List[Pod]
    runningPodList: List[Pod]

    def __init__(self):
        self.pendingPodList = []
        self.runningPodList = []
        self.deploymentList = []
        self.nodeList = []
        self.endPointList = []
