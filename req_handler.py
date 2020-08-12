import time

from api_server import APIServer
from pod import Pod
from request import Request

# reqHandler is a thread that continuously checks the pendingRequest queue and calls an associated pod to handle the incoming request.

class ReqHandler:
    apiServer: APIServer

    def __init__(self, APISERVER):
        self.apiServer = APISERVER
        self.running = True

    def __call__(self):
        print("reqHandler start")
        while self.running:
            self.apiServer.requestWaiting.wait()
            with self.apiServer.etcdLock:
                if not self.apiServer.etcd.pendingReqs: break

                request_info = self.apiServer.etcd.pendingReqs.pop()
                request = Request(request_info)
                print('[ReqHandler] Got a request to service:', request)

                candidate_endpoints = list(filter(
                    lambda endpoint: self.apiServer.CheckEndPoint(endpoint),
                    self.apiServer.GetEndPointsByLabel(request.deploymentLabel),
                ))

                for endpoint in candidate_endpoints:
                    # First try to find a pod which can service the request immediately
                    pod: Pod = endpoint.pod
                    if pod.has_capacity():
                        pod.HandleRequest(request)
                        break
                else:
                    # In reality, we'd probably put this request back onto the queue
                    # But Stephen has said it's OK to just fail the request when there isn't an available pod.
                    print('[ReqHandler] Failed to find pod to service request', request)

                self.apiServer.requestWaiting.clear()
        print("ReqHandlerShutdown")
