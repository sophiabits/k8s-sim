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
            with self.deployment.lock:
                request_infos = self.deployment.pendingReqs
                self.deployment.pendingReqs = []

            with self.api_server.etcdLock:
                candidate_endpoints = self.api_server.GetEndPointsByLabel(self.deployment.deploymentLabel)

            # TODO -- what happens if no candidate_endpoints? drop the req?

            pods = [endpoint.pod for endpoint in candidate_endpoints]
            for info in request_infos:
                request = Request(info)
                self.handle(pods, request)

        print('LoadBalancer shutdown')

    @abstractmethod
    def handle(self, pods, request): raise NotImplementedError

class RoundRobinLoadBalancer(ILoadBalancer):
    # Cycle through pods in a deployment
    def handle(self, pods, request):
        pass

class UtilizationAwareLoadBalancer(ILoadBalancer):
    # Send request to lowest utilized pod
    def handle(self, pods, request):
        pass
