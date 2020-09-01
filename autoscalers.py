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

        self.controller = PIDController(1.0, 1.0, 1.0) # TODO tune
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
            print(f'HPAScaler measuring load {self.deployment_label} {current_utilization}')
            self.measurements.append(current_utilization)

            # Step 2: See if we need to trigger an autoscale event
            if self.measurements.is_full():
                # min_bound = self.set_point * (1 - SETPOINT_BUFFER)
                # max_bound = self.set_point * (1 + SETPOINT_BUFFER)

                measured_utilization = self.measurements.average()
                error = self.controller.work(measured_utilization - self.set_point)

                print(f'HPAScaler error:', self.deployment_label, error)

                # Simulate cooldown period
                self.measurements.clear()

                '''
                If there were any missing metrics, we recompute the average more conservatively, assuming those pods were consuming 100% of the desired value in case of a scale down, and 0% in case of a scale up. This dampens the magnitude of any potential scale.
                Furthermore, if any not-yet-ready pods were present, and we would have scaled up without factoring in missing metrics or not-yet-ready pods, we conservatively assume the not-yet-ready pods are consuming 0% of the desired metric, further dampening the magnitude of a scale up.
                '''

            time.sleep(self.time)
        print('HPAScalerShutDown')

def measure_utilization(endpoints):
    n = 0
    total_utilization = 0

    for endpoint in endpoints:
        # As per k8s (https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/):
        # "All Pods with a deletion timestamp set (i.e. Pods in the process of being shut down) and all failed Pods are discarded."
        # "... if any pod has yet to become ready (i.e. it's still initializing) ..., that pod is set aside as well."
        if endpoint.pod.status != 'RUNNING': continue

        n += 1
        pod_utilization = 1 - endpoint.pod.available_cpu / endpoint.pod.assigned_cpu
        total_utilization += pod_utilization * 100

    return total_utilization / n

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
