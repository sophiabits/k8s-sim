from collections import defaultdict
import json
import os
from os.path import join as joinpath

def find_all_names(data, category):
    names = set()
    for datum in data:
        if datum['category'] == category and datum['event'] == 'created':
            names.add(datum['id'])
    return names


def select_events(data, selector: dict):
    events = []
    def is_match(datum: dict):
        for k,v in selector.items():
            if datum.get(k) != v:
                return False
        return True

    for datum in data:
        if is_match(datum):
            events.append(datum)
    return events

class MetricsProcessor:
    def __init__(self, metrics_path = './metrics.json'):
        self.data = json.loads(open(metrics_path, 'r').read())
        self.all_node_names = find_all_names(self.data, 'node')
        self.all_request_ids = find_all_names(self.data, 'request')

    def cpu_usage(self):
        # map from 't' to node cpu
        cpu_usage = {}

        for datum in self.data:
            if datum['category'] == 'node' and datum['event'] == 'cpu':
                if datum['t'] not in cpu_usage:
                    cpu_usage[datum['t']] = defaultdict(lambda: 0)
                cpu_usage[datum['t']][datum['id']] = 1 - (datum['available_cpu'] / datum['assigned_cpu'])

        # t, node_a, ...
        rows = [
            ['t', *self.all_node_names],
        ]
        for t, usage_stats in cpu_usage.items():
            row = [t]
            for node_name in self.all_node_names:
                row.append(usage_stats[node_name])
            rows.append(row)
        return rows

    def requests_plot(self):
        # [request_id, created_at, started_at, finished_at, was_success]
        requests = [
            ['Request ID', 'Created At', 'Started At', 'Finished At', 'Success?'],
        ]

        def find_event(events, event_type):
            for event in events:
                if event['event'] == event_type:
                    return event
            return None

        for request_id in self.all_request_ids:
            events = select_events(self.data, {'category': 'request', 'id': request_id})

            did_succeed = find_event(events, 'success') is not None
            creation_event = find_event(events, 'created')
            started_event = find_event(events, 'started') or {}
            finished_event = find_event(events, 'success') or find_event(events, 'failed') or {}

            requests.append([request_id, creation_event['t'], started_event.get('t'), finished_event.get('t'), did_succeed])

        return requests

    def requests_summary(self):
        error_count = 0
        # simulator run time
        max_t = max_with(self.data, lambda datum: datum['t'])

        queue_latency = 0
        queue_latency_n = 0

        requests_plot = self.requests_plot()[1:]
        for request_info in requests_plot:
            if not request_info[4]:
                error_count += 1

            if request_info[1] is not None and request_info[2] is not None:
                queue_latency += request_info[2] - request_info[1]
                queue_latency_n += 1

        return [
            ['Average request latency', queue_latency / queue_latency_n],
            ['Error count', error_count],
            ['Error rate (%)', error_count / len(requests_plot)],
            ['Request count', len(self.all_request_ids)],
            ['Requests/s', len(self.all_request_ids) / max_t],
            ['Run time', max_t],
        ]

    def deployment_distance(self):
        rows = [['t', 'Distance', 'Current', 'Expected', 'Dep #']]
        seen_t_values = set()
        for row in self.data:
            t = row['t']
            if t in seen_t_values: continue
            seen_t_values.add(t)

            dep_count = 0
            current_count = 0
            expected_count = 0

            seen_dep_names = set()
            for row2 in self.data:
                if row2['category'] != 'deployment': continue
                if row2['id'] in seen_dep_names: continue
                if row2['t'] < t: continue
                if row2['t'] > t: break

                if row2['event'] == 'created' or row2['event'] == 'replicas':
                    dep_count += 1
                    current_count += row2['current']
                    expected_count += row2['expected']
                    seen_dep_names.add(row2['id'])

            rows.append([t, expected_count - current_count, current_count, expected_count, dep_count])

        return rows

    def pod_healthiness(self):
        # [t, Pending Pods, Running Pods, Terminating Pods, Crashed Pods]
        events = [datum for datum in self.data if datum['category'] == 'pod']
        rows = [['t', 'Pending #', 'Running #', 'Terminating #', 'Crashed #']]

        pending_pods = 0
        running_pods = 0
        terminating_pods = 0
        crashed_pods = 0

        for evt in events:
            if evt['event'] == 'created':
                pending_pods += 1
            elif evt['event'] == 'started':
                pending_pods -= 1
                running_pods += 1
            elif evt['event'] == 'crashed':
                running_pods -= 1
                crashed_pods += 1
            elif evt['event'] == 'terminated':
                terminating_pods -= 1
            elif evt['event'] == 'terminating':
                if evt['old_status'] == 'PENDING':
                    pending_pods -= 1
                elif evt['old_status'] == 'RUNNING':
                    running_pods -= 1
                elif evt['old_status'] == 'FAILED':
                    crashed_pods -= 1

                terminating_pods += 1
            elif evt['event'] == 'restart':
                crashed_pods -= 1
                pending_pods += 1

            rows.append([
                evt['t'], pending_pods, running_pods, terminating_pods, crashed_pods,
            ])
        return rows

    def pod_usage(self):
        rows = [['t', 'CPU Usage (% of total)']]
        seen_t_values = set()
        for row in self.data:
            t = row['t']
            if t in seen_t_values: continue
            seen_t_values.add(t)

            assigned_cpu = 0
            available_cpu = 0
            pod_count = 0

            seen_pod_names = set()
            for row2 in self.data:
                if row2['category'] != 'pod': continue
                if row2['id'] in seen_pod_names: continue
                if row2['t'] < t: continue
                if row2['t'] > t: break

                if row2['event'] == 'status':
                    assigned_cpu += row2['assigned_cpu']
                    available_cpu += row2['available_cpu']
                    pod_count += 1
                    seen_pod_names.add(row2['id'])

            if assigned_cpu > 0:
                rows.append([t, (assigned_cpu - available_cpu) / assigned_cpu])

        return rows

def format_row(row):
    s = []
    for col in row:
        if col is None:
            s.append('')
        else:
            s.append(str(col))
    return ','.join(s) + '\n'

def max_with(iterable, fn):
    max_value = None
    for item in iterable:
        value = fn(item)
        if max_value is None or value > max_value:
            max_value = value
    return max_value


def process_many(file_paths, output_dir: str = 'stats'):
    os.makedirs(output_dir, exist_ok=True)

    processors = [MetricsProcessor(fp) for fp in file_paths]

    def write_csvs(tables, output_file):
        max_len = max_with(tables, lambda t: len(t))
        width = len(tables[0][0]) # get column count

        with open(joinpath(output_dir, output_file), 'w') as fp:
            for i in range(0, max_len):
                for table in tables:
                    if i >= len(table):
                        fp.write(',' * (width + 1))
                    else:
                        row = table[i]
                        # need to take away the \n
                        fp.write(format_row(row)[:-1] + ','*2)
                fp.write('\n')

    write_csvs(
        [p.deployment_distance() for p in processors],
        'deps.csv',
    )
    write_csvs(
        [p.pod_healthiness() for p in processors],
        'pods.csv',
    )
    write_csvs(
        [p.pod_usage() for p in processors],
        'pods_cpu.csv',
    )
    write_csvs(
        [p.requests_plot() for p in processors],
        'reqs.csv',
    )

    with open(joinpath(output_dir, 'reqs_summary'), 'w') as fp:
        for p in processors:
            for key, val in p.requests_summary():
                fp.write(f'{key}\t\t{val}\n')
            fp.write('\n\n')


def process_one(file_path: str = './metrics.json', output_dir: str = 'stats'):
    os.makedirs(output_dir, exist_ok=True)
    processor = MetricsProcessor(file_path)

    with open(joinpath(output_dir, 'deps.csv'), 'w') as fp:
        for row in processor.deployment_distance():
            fp.write(format_row(row))

    with open(joinpath(output_dir, 'pods.csv'), 'w') as fp:
        for row in processor.pod_healthiness():
            fp.write(format_row(row))

    with open(joinpath(output_dir, 'pods_cpu.csv'), 'w') as fp:
        for row in processor.pod_usage():
            fp.write(format_row(row))

    with open(joinpath(output_dir, 'reqs.csv'), 'w') as fp:
        for row in processor.requests_plot():
            fp.write(format_row(row))

    with open(joinpath(output_dir, 'reqs_summary'), 'w') as fp:
        for key, val in processor.requests_summary():
            fp.write(f'{key}\t\t{val}\n')


if __name__ == '__main__':
    process_one()
