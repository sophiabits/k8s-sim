from process_metrics import process_many
from runSimulation import main

scenarios = (
    ('tracefiles/1.txt', 'metrics_1.json'),
    ('tracefiles/2.txt', 'metrics_2.json'),
    ('tracefiles/3.txt', 'metrics_3.json'),
)

for (tracefile_path, metrics_path) in scenarios:
    main(tracefile_path, metrics_path)
    # metrics_data.append(process(metrics_path))

metric_files = [x[1] for x in scenarios]
process_many(metric_files, 'stats_batched')
