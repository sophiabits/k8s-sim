import pytest

from api_server import APIServer
from dep_controller import DepController

def count_pods_with_state(api, state):
    return sum([1 if pod.status == state else 0 for pod in api.etcd.runningPodList])

def test_scale_down():
    api = APIServer()
    dep = api.CreateDeployment(['Dep_A', 4, 1])
    node = api.CreateWorker(['Node_1', 4])
    controller = DepController(api, 5)
    controller.work()
    assert dep.currentReplicas == 4

    # Make endpoints for the pods
    assert len(api.etcd.pendingPodList) == 4
    for pod in api.GetPending():
        api.CreateEndPoint(pod, node)
    assert count_pods_with_state(api, 'RUNNING') == 4

    dep.expectedReplicas = 3
    controller.work()
    assert dep.currentReplicas == 4
    assert count_pods_with_state(api, 'TERMINATING') == 1

    # Ensure terminating pods get terminated on next invocation
    controller.work()
    assert dep.currentReplicas == 3
    assert count_pods_with_state(api, 'RUNNING') == 3
    assert count_pods_with_state(api, 'TERMINATING') == 0

    dep.expectedReplicas = 1
    controller.work()
    assert dep.currentReplicas == 3
    assert count_pods_with_state(api, 'TERMINATING') == 2
