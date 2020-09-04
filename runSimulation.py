import math
import threading
import time

import metrics
from request import Request
from autoscalers import HPA
from dep_controller import DepController
from api_server import APIServer
from node_controller import NodeController
from scheduler import Scheduler

import config
from metrics import _t_delta

def get_hpa_stats(api, hpas):
    t = _t_delta()
    stats = []
    for hpa in hpas:
        try:
            mv = hpa.measurements.average()
            if mv is math.inf:
                continue

            dep = api.GetDeploymentByLabel(hpa.deployment_label)

            def get_handled_reqs(pod):
                return pod.assigned_cpu - pod.available_cpu

            data = [
                t,
                hpa.deployment_label,
                mv,
                hpa.set_point,
                dep.currentReplicas,
                dep.expectedReplicas,
                len(dep.pendingReqs),
                sum([get_handled_reqs(endpoint.pod) for endpoint in api.GetEndPointsByLabel(hpa.deployment_label)]),
            ]
            data = [str(d) for d in data]

            stats.append(','.join(data))
        except ZeroDivisionError:
            pass

    return stats

# This is the simulation frontend that will interact with your APIServer to change cluster configurations and handle requests
# All building files are guidelines, and you are welcome to change them as much as desired so long as the required functionality is still implemented.

def main(
    instructions_file = './instructions.txt',
    metrics_file = './metrics.json',
):
    _nodeCtlLoop = 2
    _depCtlLoop = 2
    _hpaCtlLoop = 2
    _scheduleCtlLoop = 2

    disposables = []
    hpas = []
    hpa_log = ['t,Deployment,MV,SP,Current,Expected,Req#(Pending),Req#(Handling)']

    apiServer = APIServer()
    depController = DepController(apiServer, _depCtlLoop)
    nodeController = NodeController(apiServer, _nodeCtlLoop)
    scheduler = Scheduler(apiServer, _scheduleCtlLoop)
    depControllerThread = threading.Thread(name='DeploymentController', target=depController)
    nodeControllerThread = threading.Thread(name='NodeController', target=nodeController)
    schedulerThread = threading.Thread(name='Scheduler', target=scheduler)
    print('Threads Starting')
    nodeControllerThread.start()
    depControllerThread.start()
    schedulerThread.start()
    print('ReadingFile')

    with open(instructions_file, 'r') as fp:
        commands = fp.readlines()

    for command in commands:
        cmdAttributes = command.split()

        time_to_sleep = 0 # in seconds

        with apiServer.etcdLock:
            if cmdAttributes[0] == 'Deploy':
                deployment = apiServer.CreateDeployment(cmdAttributes[1:])

                load_balancer = config.load_balancer(apiServer, deployment)
                load_balancer_thread = threading.Thread(target=load_balancer)

                def cleanup():
                    load_balancer.running = False
                    load_balancer.deployment.waiting.set()
                    load_balancer_thread.join()

                load_balancer_thread.start()
                disposables.append(cleanup)
            elif cmdAttributes[0] == 'AddNode':
                apiServer.CreateWorker(cmdAttributes[1:])
            elif cmdAttributes[0] == 'CrashPod':
                apiServer.CrashPod(cmdAttributes[1])
            elif cmdAttributes[0] == 'DeleteDeployment':
                apiServer.RemoveDeployment(cmdAttributes[1])
            elif cmdAttributes[0] == 'ReqIn':
                apiServer.PushReq(cmdAttributes[1:])
            elif cmdAttributes[0] == 'CreateHPA':
                hpa = HPA(apiServer, _hpaCtlLoop, cmdAttributes[1:])
                hpa_thread = threading.Thread(target=hpa)

                def cleanup():
                    hpa.running = False
                    hpa_thread.join()

                hpas.append(hpa)
                hpa_thread.start()
                disposables.append(cleanup)
            elif cmdAttributes[0] == 'Sleep':
                time_to_sleep += int(cmdAttributes[1])

        # Get hpa measurements
        hpa_log += get_hpa_stats(apiServer, hpas)
        time.sleep(time_to_sleep)
    time.sleep(5)
    print('Shutting down threads')

    for dispose in disposables:
        dispose()

    depController.running = False
    scheduler.running = False
    nodeController.running = False
    depControllerThread.join()
    schedulerThread.join()
    nodeControllerThread.join()

    print('Recording metrics...')
    metrics.dump(metrics_file)
    with open('./hpa.csv', 'w') as fp:
        fp.write('\n'.join(hpa_log))

if __name__ == '__main__':
    main()
