import threading
import time

import metrics
from request import Request
from autoscalers import HPA
from dep_controller import DepController
from api_server import APIServer
from load_balancing import RoundRobinLoadBalancer
# from req_handler import ReqHandler
from node_controller import NodeController
from scheduler import Scheduler

# This is the simulation frontend that will interact with your APIServer to change cluster configurations and handle requests
# All building files are guidelines, and you are welcome to change them as much as desired so long as the required functionality is still implemented.

def main(
    instructions_file = './instructions.txt',
    metrics_file = './metrics.json',
    LoadBalancer = RoundRobinLoadBalancer, # load balancer implementation to use
):
    _nodeCtlLoop = 2
    _depCtlLoop = 2
    _hpaCtlLoop = 2
    _scheduleCtlLoop = 2

    disposables = []

    apiServer = APIServer()
    depController = DepController(apiServer, _depCtlLoop)
    nodeController = NodeController(apiServer, _nodeCtlLoop)
    # reqHandler = ReqHandler(apiServer)
    scheduler = Scheduler(apiServer, _scheduleCtlLoop)
    depControllerThread = threading.Thread(name='DeploymentController', target=depController)
    nodeControllerThread = threading.Thread(name='NodeController', target=nodeController)
    # reqHandlerThread = threading.Thread(name='RequestHandler', target=reqHandler)
    schedulerThread = threading.Thread(name='Scheduler', target=scheduler)
    print('Threads Starting')
    # reqHandlerThread.start()
    nodeControllerThread.start()
    depControllerThread.start()
    schedulerThread.start()
    print('ReadingFile')

    with open(instructions_file, 'r') as fp:
        commands = fp.readlines()

    for command in commands:
        cmdAttributes = command.split()

        time_to_sleep = 3 # in seconds

        with apiServer.etcdLock:
            if cmdAttributes[0] == 'Deploy':
                deployment = apiServer.CreateDeployment(cmdAttributes[1:])

                load_balancer = LoadBalancer(apiServer, deployment)
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

                hpa_thread.start()
                disposables.append(cleanup)
            elif cmdAttributes[0] == 'Sleep':
                time_to_sleep += int(cmdAttributes[1])
        time.sleep(time_to_sleep)
    time.sleep(5)
    print('Shutting down threads')

    for dispose in disposables:
        dispose()

    # reqHandler.running = False
    depController.running = False
    scheduler.running = False
    nodeController.running = False
    apiServer.requestWaiting.set()
    depControllerThread.join()
    schedulerThread.join()
    nodeControllerThread.join()
    # reqHandlerThread.join()

    print('Recording metrics...')
    metrics.dump(metrics_file)

if __name__ == '__main__':
    main()
