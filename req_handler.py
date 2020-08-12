import time

from api_server import APIServer
from pod import Pod

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

                request = self.apiServer.etcd.pendingReqs.pop()
                print('[ReqHandler] Got a request to service:', request.request_id)

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
                    print('[ReqHandler] Failed to find pod to service request', request.request_id)

                self.apiServer.requestWaiting.clear()
        print("ReqHandlerShutdown")
