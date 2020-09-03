import threading

from request import Request

# Deployment objects set the configuration and expected number of Pod objects
# label is the label associated with the deployment.
# currentReplicas is the number of pods currently running that are associated with
# the Deployment.
# expectedReplicas is the setpoint for the number of pods.
# cpuCost is the amount of cpu that a pod must be assigned.
# pendingReqs is a list of requests which are waiting to be handled for this deployment

class Deployment:
    def __init__(self, INFOLIST):
        self.deploymentLabel = INFOLIST[0]
        self.currentReplicas = 0
        self.expectedReplicas = int(INFOLIST[1])
        self.cpuCost = int(INFOLIST[2])

        self.pendingReqs = []
        self.lock = threading.Lock()
        self.waiting = threading.Event()

    def __repr__(self):
        return f'<Deployment {self.deploymentLabel}>'

    def pop_request(self):
        with self.lock:
            if self.pendingReqs:
                req = self.pendingReqs.pop(0)
                has_more_reqs = len(self.pendingReqs) > 0

                if has_more_reqs:
                    self.waiting.set()
                else:
                    self.waiting.clear()
                return Request(req)
