import math
import time

import config
from api_server import APIServer
from controllers import PIDController

SETPOINT_BUFFER = 10 # measured in points, i.e. for setpoint=50 the margin is 40<=x<=60 not 45<=x<=55

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

        # self.controller = PIDController(kp=0.85, ki=0.0013, kd=0.1) # TODO tune
        self.controller = PIDController(kp=0.9, ki=0.0013, kd=0) # TODO tune


        # self.controller = PIDController(kp=1.0, ki=1.0, kd=1.0)
        self.measurements = CappedList(max(1, math.floor(self.sync_period / loop_time)))

    def __call__(self):
        print('HPAScalerStart')
        time.sleep(config.autoscale_warmup_time)
        while self.running:
            # Step 1: Calculate utilization for this step
            deployment = self.api_server.GetDeploymentByLabel(self.deployment_label)
            if deployment is None:
                print(f'HPAScaler: ERROR -- COULD NOT FIND ASSOCIATED DEPLOYMENT')

            endpoints = self.api_server.GetEndPointsByLabel(self.deployment_label)
            current_utilization = measure_utilization(endpoints, len(deployment.pendingReqs))
            if current_utilization is not math.inf:
                # print(f'HPAScaler measuring load {self.deployment_label} {current_utilization}')
                print(f'HPAScaler({self.deployment_label}): {current_utilization}')
                self.measurements.append(current_utilization)

            # Step 2: See if we need to trigger an autoscale event
            if self.measurements.is_full():
                measured_value = self.measurements.average()

                min_bound = self.set_point - SETPOINT_BUFFER
                max_bound = self.set_point + SETPOINT_BUFFER

                # Step 2.1 Check if MV is off by >10%
                print('@@@MAX REPLICAS:', self.get_maximum_replicas(deployment.cpuCost))
                if measured_value < min_bound or measured_value > max_bound:
                    print('---')
                    print(self.deployment_label)

                    print(f'HPAScaler: Calling controller.work. MV:', measured_value, 'SP:', self.set_point)
                    error = self.controller.work(measured_value - self.set_point)
                    print(f'HPAScaler: Controller output =>', error)

                    # if error is 100, then that means we should double our expected replicas
                    # likewise if error is -100, then that means we need to kill all our pods
                    scaling_factor = error / 100

                    # this is the number of replicas we'll be destroying or creating
                    scale_magnitude = self.get_scaling_magnitude(
                        deployment.expectedReplicas,
                        scaling_factor,
                    )

                    if scale_magnitude + deployment.expectedReplicas > self.get_maximum_replicas(deployment.cpuCost):
                        print(f'HPAScaper: Skipping scale by {scale_magnitude} replicas. Would exceed max replicas {self.get_maximum_replicas(deployment.cpuCost)}')
                    elif scale_magnitude != 0:
                        print(f'HPAScaler: Maybe scaling deployment by {scale_magnitude} replicas? (ER={deployment.expectedReplicas})')

                        # # calculate the expected usage after scaling
                        # with deployment.lock:
                        #     pending_req_count = len(deployment.pendingReqs)
                        # current_cpu_count = deployment.cpuCost * deployment.currentReplicas
                        # current_cpu_used = current_cpu_count * measured_value
                        # new_cpu_count = current_cpu_count + scaling_factor * deployment.cpuCost
                        # if scale_magnitude > 0:
                        #     new_cpu_used = current_cpu_used + min(pending_req_count, scaling_factor * deployment.cpuCost)
                        # else:
                        #     new_cpu_used = current_cpu_used - scaling_factor * deployment.cpuCost
                        # estimated_value = new_cpu_used / new_cpu_count

                        # print(f'HPAScaler: Applying scaling decision estimated to change MV ->', estimated_value)

                        deployment.expectedReplicas = deployment.expectedReplicas + scale_magnitude
                        print(f'HPAScaler: new expectedReplicas=', deployment.expectedReplicas)
                    else:
                        print(f'HPAScaler: .. ignoring, results in no scale')

                    print('---')

                    # Clear old measurements and then start cooling down
                    self.measurements.clear()
                    time.sleep(4)
                else:
                    print(f'HPAScaler: MV:', measured_value, 'SP:', self.set_point, '-> no scale')

            time.sleep(self.time)
        print('HPAScalerShutDown')

    def get_maximum_replicas(self, cpu_cost):
        deployment_count = len(self.api_server.GetDeployments())
        cluster_cpus = self.api_server.GetClusterResources()['cpu']

        return math.ceil(min(
            cluster_cpus / deployment_count + 0.1 * cluster_cpus,
            cluster_cpus,
        ) / cpu_cost)

    def get_scaling_magnitude(self, expected_replicas, scaling_factor):
        # if scaling_factor == 1 -> add 100% replicas, and vice versa

        scale_magnitude = expected_replicas * scaling_factor
        if scaling_factor < 0:
            scale_magnitude = math.floor(scale_magnitude)
            if scale_magnitude <= -expected_replicas:
                # don't kill all our replicas!
                scale_magnitude = -(expected_replicas - 1)
        else:
            scale_magnitude = math.ceil(scale_magnitude)

        return scale_magnitude

def measure_utilization(endpoints, pending_req_count):
    n_discarded = 0

    total_assigned = 0
    total_available = 0

    for endpoint in endpoints:
        # As per k8s (https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/):
        # "All Pods with a deletion timestamp set (i.e. Pods in the process of being shut down) and all failed Pods are discarded."
        # "... if any pod has yet to become ready (i.e. it's still initializing) ..., that pod is set aside as well."
        if endpoint.pod.status == 'RUNNING':
            total_assigned += endpoint.pod.assigned_cpu
            total_available += endpoint.pod.available_cpu
        elif endpoint.pod.status == 'PENDING':
            total_assigned += endpoint.pod.assigned_cpu

    if total_assigned == 0:
        return math.inf

    total_used = total_assigned - total_available + pending_req_count
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
