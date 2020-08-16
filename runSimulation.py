import threading
import time

import metrics
from request import Request
from dep_controller import DepController
from api_server import APIServer
from req_handler import ReqHandler
from node_controller import NodeController
from scheduler import Scheduler

# This is the simulation frontend that will interact with your APIServer to change cluster configurations and handle requests
# All building files are guidelines, and you are welcome to change them as much as desired so long as the required functionality is still implemented.

def main(instructions_file = './instructions.txt', metrics_file = './metrics.json'):
    _nodeCtlLoop = 5
    _depCtlLoop = 5
    _scheduleCtlLoop = 5

    apiServer = APIServer()
    depController = DepController(apiServer, _depCtlLoop)
    nodeController = NodeController(apiServer, _nodeCtlLoop)
    reqHandler = ReqHandler(apiServer)
    scheduler = Scheduler(apiServer, _scheduleCtlLoop)
    depControllerThread = threading.Thread(name='DeploymentController', target=depController)
    nodeControllerThread = threading.Thread(name='NodeController', target=nodeController)
    reqHandlerThread = threading.Thread(name='RequestHandler', target=reqHandler)
    schedulerThread = threading.Thread(name='Scheduler', target=scheduler)
    print('Threads Starting')
    reqHandlerThread.start()
    nodeControllerThread.start()
    depControllerThread.start()
    schedulerThread.start()
    print('ReadingFile')

    with open(instructions_file, 'r') as fp:
        commands = fp.readlines()

    for command in commands:
        cmdAttributes = command.split()

        # Note: Originally, simulator slept for 5s between each command. Is this a requirement?
        time_to_sleep = 0 # in seconds

        with apiServer.etcdLock:
            if cmdAttributes[0] == 'Deploy':
                apiServer.CreateDeployment(cmdAttributes[1:])
            elif cmdAttributes[0] == 'AddNode':
                apiServer.CreateWorker(cmdAttributes[1:])
            elif cmdAttributes[0] == 'CrashPod':
                apiServer.CrashPod(cmdAttributes[1])
            elif cmdAttributes[0] == 'DeleteDeployment':
                apiServer.RemoveDeployment(cmdAttributes[1])
            elif cmdAttributes[0] == 'ReqIn':
                apiServer.PushReq(cmdAttributes[1:])
            elif cmdAttributes[0] == 'Sleep':
                time_to_sleep = int(cmdAttributes[1])
        time.sleep(5 + time_to_sleep)
    time.sleep(5)
    print('Shutting down threads')

    reqHandler.running = False
    depController.running = False
    scheduler.running = False
    nodeController.running = False
    apiServer.requestWaiting.set()
    depControllerThread.join()
    schedulerThread.join()
    nodeControllerThread.join()
    reqHandlerThread.join()

    print('Recording metrics...')
    metrics.dump(metrics_file)

if __name__ == '__main__':
    main()
