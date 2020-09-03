import math
import time

from api_server import APIServer
from controllers import PIDController

SETPOINT_BUFFER = 0.1 # +- 10%

''' Horizontal pod autoscaler '''
class HPA:
    api_server: APIServer
    deployment_label: str
    set_point: int
    sync_period: int

    def __init__(self, api_server, loop_time, info):
        self.api_server = api_server
        self.running = True
        self.time = loop_time

        self.deployment_label = info[0]
        self.set_point = int(info[1])
        self.sync_period = int(info[2])

        self.controller = PIDController(kp=1.0, ki=1.0, kd=1.0) # TODO tune
        measurements_len = max(
            1,
            math.floor(self.sync_period / loop_time),
        )
        self.measurements = CappedList(math.floor(self.sync_period / loop_time))

    def __call__(self):
        print('HPAScalerStart')
        while self.running:
            # Step 1: Calculate utilization for this step
            endpoints = self.api_server.GetEndPointsByLabel(self.deployment_label)
            current_utilization = measure_utilization(endpoints)
            if current_utilization is not math.inf:
                # print(f'HPAScaler measuring load {self.deployment_label} {current_utilization}')
                self.measurements.append(current_utilization)

            # Step 2: See if we need to trigger an autoscale event
            if self.measurements.is_full():
                measured_value = self.measurements.average()

                min_bound = self.set_point * (1 - SETPOINT_BUFFER)
                max_bound = self.set_point * (1 + SETPOINT_BUFFER)

                # Step 2.1 Check if MV is off by >10%
                if measured_value < min_bound or measured_value > max_bound:
                    print('---')

                    print(f'HPAScaler: Calling controller.work. MV:', measured_value, 'SP:', self.set_point)
                    error = self.controller.work(measured_value - self.set_point)
                    print(f'HPAScaler: Controller output =>', error)

                    deployment = self.api_server.GetDeploymentByLabel(self.deployment_label)
                    if deployment is None:
                        print(f'HPAScaler: ERROR -- COULD NOT FIND ASSOCIATED DEPLOYMENT')

                    # if error is 100, then that means we should double our expected replicas
                    # likewise if error is -100, then that means we need to kill all our pods
                    scaling_factor = error / 100

                    # dampen the magnitude of scaling decisions by rounding toward zero
                    rounding_fn = math.ceil if scaling_factor < 0 else math.floor

                    # this is the number of replicas we'll be destroying or creating
                    scale_magnitude = rounding_fn(deployment.expectedReplicas * scaling_factor)

                    print(f'HPAScaler: Maybe scaling deployment by {scale_magnitude} replicas?')

                    deployment.expectedReplicas = max(deployment.expectedReplicas + scale_magnitude, 1)
                    print(f'HPAScaler: new expectedReplicas=', deployment.expectedReplicas)

                    print('---')

                    # Clear old measurements and then start cooling down
                    self.measurements.clear()
                    time.sleep(4)
                else:
                    print(f'HPAScaler: MV:', measured_value, 'SP:', self.set_point, '-> no scale')

            time.sleep(self.time)
        print('HPAScalerShutDown')

def measure_utilization(endpoints):
    n_discarded = 0

    total_assigned = 0
    total_used = 0

    for endpoint in endpoints:
        # As per k8s (https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/):
        # "All Pods with a deletion timestamp set (i.e. Pods in the process of being shut down) and all failed Pods are discarded."
        # "... if any pod has yet to become ready (i.e. it's still initializing) ..., that pod is set aside as well."
        if endpoint.pod.status == 'RUNNING':
            total_assigned += endpoint.pod.assigned_cpu
            total_used += endpoint.pod.available_cpu
        elif endpoint.pod.status == 'PENDING':
            total_assigned += endpoint.pod.assigned_cpu

    if total_assigned == 0:
        return math.inf

    return total_used / total_assigned * 100

class CappedList:
    def __init__(self, max_size: int):
        self.data = []
        self.max_size = max_size

    def append(self, value):
        if self.is_full():
            self.data.pop(0)
        self.data.append(value)

    def average(self):
        return sum(self.data) / len(self.data)

    def clear(self):
        self.data = []

    def is_full(self):
        return len(self.data) >= self.max_size
