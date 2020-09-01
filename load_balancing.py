from abc import ABCMeta, abstractmethod

from api_server import APIServer
from deployment import Deployment
from request import Request

class ILoadBalancer:
    __metaclass__ = ABCMeta

    api_server: APIServer
    deployment: Deployment

    def __init__(self, api_server, deployment):
        self.api_server = api_server
        self.deployment = deployment
        self.running = True

    def __call__(self):
        print('LoadBalancer start')
        while self.running:
            self.deployment.waiting.wait()
            self.deployment.waiting.clear()

            with self.api_server.etcdLock:
                candidate_endpoints = self.api_server.GetEndPointsByLabel(self.deployment.deploymentLabel)

            pods = [endpoint.pod for endpoint in candidate_endpoints]
            if not pods:
                # No pods available to handle the request for this deployment, so we'll skip this iteration
                # TODO -- does this impl actually work?
                # TODO -- metric
                self.deployment.waiting.set()
                continue

            with self.deployment.lock:
                request_infos = self.deployment.pendingReqs
                self.deployment.pendingReqs = []

            for info in request_infos:
                request = Request(info)
                self.handle(pods, request)

        print('LoadBalancer shutdown')

    @abstractmethod
    def handle(self, pods, request): raise NotImplementedError

class RoundRobinLoadBalancer(ILoadBalancer):
    ''' Tracks which pod index to assign the next incoming request to. '''
    current_index = 0

    # Cycle through pods in a deployment
    def handle(self, pods, request):
        if self.current_index >= len(pods):
            # Wrap around back to the first pod
            self.current_index = 0

        pod = pods[self.current_index]
        metrics.request_routed(pod, request)
        pod.HandleRequest(request)

        self.current_index += 1

class UtilizationAwareLoadBalancer(ILoadBalancer):
    # Send request to lowest utilized pod
    def handle(self, pods, request):
        pass
