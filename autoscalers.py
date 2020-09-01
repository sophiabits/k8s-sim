from api_server import APIServer

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

    def __call__(self):
        print('HPAScalerStart')
        while self.running:
            # TODO
            time.sleep(self.time)
        print('HPAScalerShutDown')
