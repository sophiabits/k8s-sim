import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading

import metrics
from request import Request

# The Pod is the unit of scaling within Kubernetes. It encapsulates the running containerized application
# name is the name of the Pod. This is the deploymentLabel with the replica number as a suffix. ie "deploymentA1"
# assigned_cpu is the cost in cpu of deploying a Pod onto a Node
# available_cpu is how many cpu threads are currently available
# deploymentLabel is the label of the Deployment that the Pod is managed by
# status is a string that communicates the Pod's availability. ['PENDING','RUNNING', 'TERMINATING', 'FAILED']
# the pool is the threads that are available for request handling on the pod
class Pod:
    def __init__(self, NAME, ASSIGNED_CPU, DEPLABEL):
        self.podName = NAME
        self.assigned_cpu = ASSIGNED_CPU
        self.available_cpu = ASSIGNED_CPU # THIS NEEDS TO BE MODIFIED WHEN THREADS ARE RUNNING
        self.deploymentLabel = DEPLABEL
        self.status = 'PENDING'
        self.crash = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=ASSIGNED_CPU)

        self._futures = []

    def __repr__(self):
        return f'<Pod {self.podName}>'

    def has_capacity(self):
        # print(f'[Pod] {self} has capacity? {self.available_cpu > 0}')
        return self.available_cpu > 0

    def is_running(self):
        return not all([f.done() for f in self._futures])

    def HandleRequest(self, request: Request):
        def handler():
            metrics.request_started(self, request)

            self.available_cpu -= 1
            crashed = self.crash.wait(timeout=request.execTime)
            self.available_cpu += 1

            if crashed:
                metrics.request_failed(self, request)
            else:
                metrics.request_success(self, request)

        self._futures.append(self.pool.submit(handler))
