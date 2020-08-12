# EndPoint objects associate a Pod with a Deployment and a Node.
# podName is the name of the Pod.
# deploymentLabel is the label of the Deployment.
# nodeLabel is the label of the Node.

class EndPoint:
    def __init__(self, POD, DEPLABEL, NODE):
        self.pod = POD
        self.deploymentLabel = DEPLABEL
        self.node = NODE

        # Priority of the endpoint for request routing [0, 1]
        # Unused for this assignment.
        # self.flag = 0

    def __repr__(self):
        return f'<EndPoint {self.pod} <-> {self.node}>'
