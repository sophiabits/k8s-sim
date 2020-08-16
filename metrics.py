# import datetime.datetime
from datetime import datetime
import json
import math
import threading

_records = []
_START_TIME = datetime.now()

def _t_delta():
    dt = datetime.now()
    return math.floor((dt - _START_TIME).total_seconds())


_empty_data = {}
def _push(category, event, id, data=_empty_data):
    current_thread = threading.current_thread()
    t = _t_delta()

    _records.append({
        **data,
        'category': category, # e.g. 'pod' or 'deployment'
        'event': event,       # e.g. 'started'
        'id': id,             # e.g. 'DEPLOYMENT_AD'
        # 'type': event_type,
        'source': 'Simulator' if current_thread.name == 'MainThread' else current_thread.name,
        't': t,
    })

    stringified_data = '' if data is _empty_data else f' {json.dumps(data)}'
    print(f'[t={t}\t] {category}.{event} @ {id}{stringified_data}')


def dump(output_file):
    with open(output_file, 'w') as fp:
        fp.write(json.dumps(_records, indent=2, separators=(',', ': ')))


# Deployment lifecycle
#   created --> replicas --> request_deletion --> deleted
#                ^----/
def deployment_created(deployment):
    _push('deployment', 'created', deployment.deploymentLabel, {
        'current': 0,
        'expected': deployment.expectedReplicas,
    })

def deployment_deleted(deployment):
    _push('deployment', 'deleted', deployment.deploymentLabel, {
        'current': deployment.currentReplicas,
        'expected': deployment.expectedReplicas,
    })

def deployment_request_deletion(deployment):
    _push('deployment', 'request_deletion', deployment.deploymentLabel, {
        'current': deployment.currentReplicas,
        'expected': deployment.expectedReplicas,
    })

def deployment_replicas(deployment):
    _push('deployment', 'replicas', deployment.deploymentLabel, {
        'current': deployment.currentReplicas,
        'expected': deployment.expectedReplicas,
    })


def node_created(node):
    _push('node', 'created', node.label)

def node_cpu(node):
    _push('node', 'cpu', node.label, {
        'assigned_cpu': node.assigned_cpu,
        'available_cpu': node.available_cpu,
    })


def pod_crashed(pod):
    _push('pod', 'crashed', pod.podName, { 'old_status': pod.status })

def pod_created(pod):
    _push('pod', 'created', pod.podName)

def pod_restart(pod):
    # Crashed pod transitioned back to PENDING
    _push('pod', 'restart', pod.podName, { 'old_status': pod.status })

def pod_terminated(pod):
    _push('pod', 'terminated', pod.podName, { 'old_status': pod.status })

def pod_terminating(pod):
    _push('pod', 'terminating', pod.podName, { 'old_status': pod.status })

def pod_started(pod, was_crashed = False):
    _push('pod', 'started', pod.podName, { 'was_crashed': was_crashed })

def pod_status(pod):
    _push('pod', 'status', pod.podName, {
        'assigned_cpu': pod.assigned_cpu,
        'available_cpu': pod.available_cpu,
        'status': pod.status,
    })


# Request lifecycle
#    created -> routed------>started-->success
#          |         |             |
#          |         |---------------->failed
#          |--> not_routed
def request_created(request_id):
    _push('request', 'created', request_id)

def request_failed(pod, request):
    _push('request', 'failed', request.request_id, {
        'pod': pod.podName,
    })

def request_not_routed(request):
    _push('request', 'not_routed', request.request_id)

def request_routed(pod, request):
    _push('request', 'routed', request.request_id, {
        'pod': pod.podName,
    })

def request_started(pod, request):
    _push('request', 'started', request.request_id, {
        'pod': pod.podName,
    })

def request_success(pod, request):
    _push('request', 'success', request.request_id, {
        'pod': pod.podName,
    })
