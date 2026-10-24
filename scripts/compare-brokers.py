#!/usr/bin/env python3
"""Paired, verified single-broker experiment. Never production qualification."""
import argparse
import json
import os
import platform
import queue
import random
import signal
import statistics
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--go-app', required=True)
parser.add_argument('--driver', required=True)
parser.add_argument('--classpath', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--brokers', default='localhost:9092')
parser.add_argument('--profile-go', action='store_true', help='Go-only diagnostic profiles; no comparative result')
args = parser.parse_args()
out = Path(args.output).resolve(); out.mkdir(parents=True, exist_ok=True)
if any(out.iterdir()):
    raise SystemExit('Use an empty output directory to preserve separate measurement runs')
records = int(os.getenv('KSTREAM_BROKER_RECORDS', '200000'))
warmup = int(os.getenv('KSTREAM_BROKER_WARMUP', '100000'))
trials = int(os.getenv('KSTREAM_BROKER_TRIALS', '1' if args.profile_go else '5'))
if records < 1 or warmup < 0 or trials < (1 if args.profile_go else 5):
    raise SystemExit('positive records, nonnegative warmup and >=5 comparative trials (>=1 diagnostic) required')
allowed = sorted(os.sched_getaffinity(0))
app_cpu = allowed[0]
driver_cpu = allowed[-1] if len(allowed) > 1 else app_cpu
manifest = {
    'reference': 'Kafka Streams 3.9.1', 'broker': 'Kafka 3.9.1, one node, replication factor 1',
    'semantics': 'Kafka transactions: committed output/changelog/group offsets',
    'cache': 'rebuildable persistent local caches; Go WAL disabled, no local sync',
    'source_partitions': 3, 'keys': 64, 'input': 'uniform keys, continuous decimal sequence values',
    'records': records, 'warmup': warmup, 'trials': trials, 'scenarios': ['filter', 'count'],
    'max_poll_records': 4096, 'go_commit': 'after each bounded poll', 'kafka_commit_interval_ms': 10,
    'compression': 'none', 'app_cpu_affinity': [app_cpu], 'driver_cpu_affinity': [driver_cpu],
    'heap_targets': 'Go GOMEMLIMIT=512MiB; Java -Xms32m -Xmx512m, active processors 1',
    'hard_rss_cap': False, 'broker_cpu_isolated': False, 'go_batch_observation': True,
    'latency': 'external driver monotonic send ledger to read-committed output receipt',
    'cpu_window': 'observed around driver measurement events, before offset/tail verification; approximate',
    'rss_window': 'process high-water mark through measurement completion; shutdown excluded',
    'host': platform.platform(), 'allowed_cpus': allowed,
    'production_qualified': False,
    'diagnostic_go_profiles': args.profile_go,
    'limits': 'Shared hosted runner; one broker; small state; no HA/failure/recovery throughput; different commit policies.'
}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
environment = subprocess.run(['lscpu'], capture_output=True, text=True, check=True).stdout
environment += subprocess.run(['go', 'version'], capture_output=True, text=True, check=True).stdout
environment += subprocess.run(['java', '-version'], capture_output=True, text=True, check=True).stderr
environment += subprocess.run(['go', 'version', '-m', args.go_app], capture_output=True, text=True, check=True).stdout
(out / 'environment.txt').write_text(environment)
env = dict(os.environ, GOMAXPROCS='1', GOMEMLIMIT='512MiB')
ticks = os.sysconf('SC_CLK_TCK')

def resource(pid):
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
    status = Path(f'/proc/{pid}/status').read_text().splitlines()
    hwm = next(int(line.split()[1]) for line in status if line.startswith('VmHWM:'))
    return {'cpu_ns': int((int(stat[11]) + int(stat[12])) * 1_000_000_000 / ticks),
            'peak_rss_kib': hwm, 'observed_monotonic_ns': time.monotonic_ns()}

def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        code = process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise RuntimeError('process did not shut down within 20 seconds')
    if code not in (0, 143, -signal.SIGTERM):
        raise RuntimeError(f'process failed during shutdown: {code}')

samples = []
run_id = uuid.uuid4().hex[:10]
for scenario in manifest['scenarios']:
    for trial in range(trials):
        paired = []
        order = ['go'] if args.profile_go else (['go', 'kafka'] if trial % 2 == 0 else ['kafka', 'go'])
        for engine in order:
            name = f'{scenario}-{trial}-{engine}'
            application = f'ksbench-{run_id}-{name}'
            source, sink = application + '-in', application + '-out'
            changelog = application + ('-counts-changelog' if engine == 'kafka' else '-state')
            common = ['-brokers', args.brokers, '-application', application, '-input', source,
                      '-output', sink, '-changelog', changelog, '-scenario', scenario,
                      '-records', str(records), '-warmup', str(warmup), '-partitions', '3', '-keys', '64']
            control = ['taskset', '-c', str(driver_cpu), args.driver]
            app = driver = None
            before = after = result = warmup_result = None
            with tempfile.TemporaryDirectory(prefix='kstream-broker-case-') as directory, \
                    (out / f'{name}.app.log').open('w') as app_log, \
                    (out / f'{name}.driver.log').open('w') as driver_log, \
                    (out / f'{name}.admin.log').open('w') as admin_log:
                subprocess.run(control + ['init'] + common + ['-timeout', '30s'], env=env,
                               stdout=admin_log, stderr=admin_log, check=True, timeout=40)
                try:
                    if engine == 'go':
                        command = [args.go_app, '-scenario', scenario, '-brokers', args.brokers,
                                   '-application', application, '-input', source, '-output', sink,
                                   '-changelog', changelog, '-state', directory, '-warmup', str(warmup)]
                        if args.profile_go:
                            command += ['-cpuprofile', str(out / f'{name}.cpu.pprof'),
                                        '-heapprofile', str(out / f'{name}.heap.pprof')]
                    else:
                        command = ['java', '-XX:ActiveProcessorCount=1', '-Xms32m', '-Xmx512m',
                                   '-cp', args.classpath, 'dev.kstream.BrokerApplication', scenario,
                                   args.brokers, application, source, sink, directory]
                    command = ['taskset', '-c', str(app_cpu)] + command
                    (out / f'{name}.command.json').write_text(json.dumps(command, indent=2) + '\n')
                    app = subprocess.Popen(command, env=env, stdout=app_log, stderr=app_log,
                                           start_new_session=True)
                    driver = subprocess.Popen(control + ['run'] + common + ['-timeout', '180s'],
                                              env=env, stdout=subprocess.PIPE, stderr=driver_log,
                                              text=True, start_new_session=True)
                    events = queue.Queue(maxsize=8)
                    def read_events(stream):
                        for line in stream:
                            events.put(line)
                        events.put(None)
                    thread = threading.Thread(target=read_events, args=(driver.stdout,), daemon=True)
                    thread.start()
                    while True:
                        if app.poll() is not None:
                            raise RuntimeError(f'{name}: application exited early: {app.returncode}; see app log')
                        try:
                            line = events.get(timeout=0.1)
                        except queue.Empty:
                            continue
                        if line is None:
                            break
                        event = json.loads(line)
                        if event['event'] == 'warmup_done':
                            before = resource(app.pid)
                            warmup_result = event['result']
                        elif event['event'] == 'measurement_output_done':
                            after = resource(app.pid)
                        elif event['event'] == 'measurement_done':
                            result = event
                        else:
                            raise RuntimeError('unexpected driver event: ' + line)
                    code = driver.wait(timeout=10)
                    thread.join(timeout=1)
                    if code or before is None or after is None or result is None:
                        raise RuntimeError(f'{name}: incomplete verified sample, driver exit {code}; see driver log')
                    if not result['group_offsets_verified'] or result['result']['records'] != records:
                        raise RuntimeError('unverified output sample')
                    sample = {'engine': engine, 'scenario': scenario, 'trial': trial,
                              'application': application, 'result': result['result'],
                              'warmup_result': warmup_result,
                              'group_offsets_verified': True, 'source_offsets': result['source_offsets'],
                              'resource_before': before, 'resource_after': after,
                              'observed_measurement_cpu_ns': after['cpu_ns'] - before['cpu_ns'],
                              'peak_rss_kib': after['peak_rss_kib']}
                finally:
                    try:
                        stop(driver)
                    finally:
                        try:
                            stop(app)
                        finally:
                            subprocess.run(control + ['delete'] + common + ['-timeout', '30s'], env=env,
                                           stdout=admin_log, stderr=admin_log, check=True, timeout=40)
                if engine == 'go':
                    app_log.flush()
                    observations = []
                    for line in (out / f'{name}.app.log').read_text().splitlines():
                        try:
                            event = json.loads(line)
                        except ValueError:
                            continue
                        if isinstance(event, dict) and event.get('event') == 'batch_metrics':
                            observations.append(event)
                    if len(observations) != 1 or observations[0]['boundary_crossed']:
                        raise RuntimeError('missing or mixed batch observation phase')
                    observation = observations[0]
                    if observation['warmup']['records'] != warmup or observation['measurement']['records'] != records:
                        raise RuntimeError('batch observation differs from verified input count')
                    sample['batch_observation'] = observation
                    if args.profile_go:
                        for profile, sample_index in [('cpu', 'cpu'), ('heap', 'alloc_space'), ('heap', 'alloc_objects'), ('heap', 'inuse_space')]:
                            command = ['go', 'tool', 'pprof', '-top', '-cum', '-nodecount=60',
                                       '-sample_index=' + sample_index, args.go_app, str(out / f'{name}.{profile}.pprof')]
                            report = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
                            (out / f'{name}.{sample_index}.txt').write_text(report.stdout + report.stderr)
                samples.append(sample)
                paired.append(sample['result']['checksum'])
                with (out / 'samples.jsonl').open('a') as output:
                    output.write(json.dumps(sample) + '\n')
                print(json.dumps({'case': name, 'records_per_second': records * 1e9 / sample['result']['nanos'],
                                  'p99_ms': sample['result']['p99_latency_ns'] / 1e6,
                                  'peak_rss_kib': sample['peak_rss_kib']}), flush=True)
        if len(set(paired)) != 1:
            raise RuntimeError(f'{scenario}/{trial}: engine output checksum mismatch')

summary = {'production_qualified': False, 'target_throughput_ratio': 2.5, 'scenarios': {}}
if args.profile_go:
    # Profiling perturbs timing. Never enter the paired-comparison calculation.
    summary = {'production_qualified': False, 'diagnostic_only': True,
               'profile_scope': 'whole process; startup, warmup, measurement and shutdown',
               'heap_scope': 'post-cleanup GC; alloc_space/alloc_objects cumulative, inuse_space not peak RSS',
               'verified_cases': len(samples)}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)
    raise SystemExit(0)
rng = random.Random(73019)
for scenario in manifest['scenarios']:
    by = {(sample['engine'], sample['trial']): sample for sample in samples if sample['scenario'] == scenario}
    ratios = [by['kafka', i]['result']['nanos'] / by['go', i]['result']['nanos'] for i in range(trials)]
    row = {'paired_throughput_ratios': ratios, 'median_throughput_ratio': statistics.median(ratios)}
    bootstrap = sorted(statistics.median(rng.choices(ratios, k=trials)) for _ in range(10000))
    row['exploratory_bootstrap_95pct'] = [bootstrap[250], bootstrap[9749]]
    for engine in ['go', 'kafka']:
        rows = [by[engine, i] for i in range(trials)]
        row[engine] = {'median_records_per_second': statistics.median(records * 1e9 / r['result']['nanos'] for r in rows),
                       'median_p99_latency_ms': statistics.median(r['result']['p99_latency_ns'] / 1e6 for r in rows),
                       'median_peak_rss_kib': statistics.median(r['peak_rss_kib'] for r in rows),
                       'median_observed_cpu_ns': statistics.median(r['observed_measurement_cpu_ns'] for r in rows),
                       'median_source_produce_fraction': statistics.median(r['result']['source_produce_nanos'] / r['result']['nanos'] for r in rows)}
    summary['scenarios'][scenario] = row
    row['go']['median_processing_batches'] = statistics.median(by['go', i]['batch_observation']['measurement']['batches'] for i in range(trials))
    row['go']['median_records_per_batch'] = statistics.median(records / by['go', i]['batch_observation']['measurement']['batches'] for i in range(trials))
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
