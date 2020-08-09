import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading

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
        self.available_cpu = ASSIGNED_CPU
        self.deploymentLabel = DEPLABEL
        self.status = 'PENDING'
        self.crash = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=ASSIGNED_CPU)
        self.threads_running = 0

    def has_capacity(self):
        print(f'[Pod] {self.podName} is checking capacity {self.threads_running}/{self.available_cpu}')
        return self.threads_running < self.available_cpu

    def HandleRequest(self, request: Request):
        def handler():
            self.threads_running += 1
            print('[Pod] servicing request:', request.request_id)
            crashed = self.crash.wait(timeout=request.execTime)
            if crashed:
                # TODO -- log that this request crashed
                print('[Pod] Request crashed during handling!', self.podName, request.request_id)
            else:
                print('[Pod] finished request', request.request_id)

            self.threads_running -= 1

        self.pool.submit(handler)
