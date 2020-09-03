from abc import ABCMeta, abstractmethod
import time

from api_server import APIServer
from deployment import Deployment
import metrics

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

            with self.api_server.etcdLock:
                candidate_endpoints = list(filter(
                    lambda endpoint: self.api_server.CheckEndPoint(endpoint),
                    self.api_server.GetEndPointsByLabel(self.deployment.deploymentLabel),
                ))

            pods = [endpoint.pod for endpoint in candidate_endpoints]
            if pods:
                pod = self.route(pods)
                if not pod or pod.available_cpu == 0:
                    # No available pod
                    continue

                request = self.deployment.pop_request()
                if not request:
                    continue # No request waiting

                metrics.request_routed(pod, request)
                pod.HandleRequest(request)

        print('LoadBalancer shutdown')

    @abstractmethod
    def route(self, pods): raise NotImplementedError


class FirstFitLoadBalancer(ILoadBalancer):
    def route(self, pods):
        for pod in pods:
            if pod.available_cpu > 0:
                return pod


class RoundRobinLoadBalancer(ILoadBalancer):
    ''' Tracks which pod index to assign the next incoming request to. '''
    current_index = 0

    # Cycle through pods in a deployment
    def route(self, pods):
        if self.current_index >= len(pods):
            # Wrap around back to the first pod
            self.current_index = 0

        pod = pods[self.current_index]
        self.current_index += 1
        return pod


class UtilizationAwareLoadBalancer(ILoadBalancer):
    # Send request to lowest utilized pod
    def route(self, pods):
        pods_by_cpu = sorted(
            pods,
            key=lambda pod: pod.available_cpu,
            reverse=True,
        )

        return pods_by_cpu[0]
