import config

# The WorkerNode is the object to which pods can be scheduled
# label is the label of the Node
# assigned_cpu is the amount of cpu assigned to the node
# available_cpu is the amount of assigned_cpu not currently in use
# status communicates the Node's availability. ['UP', 'CRASHED', 'TERMINATING', 'TERMINATED']

class WorkerNode:
    def __init__(self, INFOLIST):
        self.label = INFOLIST[0]
        self.assigned_cpu = int(INFOLIST[1]) * config.cpu_scale_factor
        self.available_cpu = self.assigned_cpu * config.cpu_scale_factor
        self.status = 'UP'

    def __repr__(self):
        return f'<WorkerNode {self.label}>'
