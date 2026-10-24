#!/usr/bin/env python3
"""Balanced, verified Go-only fetch-target experiment on one host."""
import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--go-app', required=True)
parser.add_argument('--driver', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--brokers', default='localhost:9092')
parser.add_argument('--rounds', type=int, default=3)
args = parser.parse_args()
if args.rounds < 3 or args.rounds % 3:
    parser.error('--rounds must be a positive multiple of three, at least three')
out = Path(args.output).resolve()
out.mkdir(parents=True, exist_ok=True)
if any(out.iterdir()):
    parser.error('use an empty output directory to preserve previous evidence')

budgets = [8 << 20, 1 << 20, 256 << 10]
plan = [budgets[r % 3:] + budgets[:r % 3] for r in range(args.rounds)]
env = dict(os.environ)
env.setdefault('KSTREAM_BROKER_RECORDS', '1000000')
env.setdefault('KSTREAM_BROKER_WARMUP', '1000000')
env.setdefault('KSTREAM_BROKER_OUTPUT_CAPACITY', '4096')
env['KSTREAM_BROKER_TRIALS'] = '1'
manifest = {
    'diagnostic_only': True, 'instrumented': False, 'production_qualified': False,
    'scope': 'Go-only configuration experiment; no Kafka Streams throughput ratio',
    'fetch_targets': budgets, 'round_order': plan, 'rounds': args.rounds,
    'records': int(env['KSTREAM_BROKER_RECORDS']), 'warmup': int(env['KSTREAM_BROKER_WARMUP']),
    'output_capacity': int(env['KSTREAM_BROKER_OUTPUT_CAPACITY']),
    'app_sha256': hashlib.sha256(Path(args.go_app).read_bytes()).hexdigest(),
    'driver_sha256': hashlib.sha256(Path(args.driver).read_bytes()).hexdigest(),
    'default_fetch_bytes_unchanged': 8 << 20,
    'limits': 'Shared CPUs/unisolated broker; tiny values and state; soft heap target; no large-payload/HA qualification.'
}
(out / 'matrix-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
by = {}
script = Path(__file__).with_name('compare-brokers.py')
for round_id, order in enumerate(plan):
    for budget in order:
        leaf = out / f'round-{round_id}' / f'fetch-{budget}'
        print(json.dumps({'round': round_id, 'fetch_bytes': budget}), flush=True)
        subprocess.run([sys.executable, str(script), '--go-only', '--go-app', args.go_app,
                        '--driver', args.driver, '--output', str(leaf), '--brokers', args.brokers],
                       env=env | {'KSTREAM_BROKER_FETCH_BYTES': str(budget)}, check=True)
        settings = json.loads((leaf / 'manifest.json').read_text())
        summary = json.loads((leaf / 'summary.json').read_text())
        if (settings['comparison_engines'] != ['go'] or settings['diagnostic_go_profiles'] or
                not settings['uninstrumented_go_only'] or settings['go_fetch_bytes'] != budget or
                settings['trials'] != 1 or settings['records'] != manifest['records'] or
                settings['warmup'] != manifest['warmup'] or
                settings['go_output_capacity'] != manifest['output_capacity'] or
                not summary['diagnostic_only'] or summary['instrumented'] or
                summary['verified_cases'] != 2):
            raise RuntimeError('configuration/instrumentation mismatch; refusing to compare samples')
        samples = [json.loads(line) for line in (leaf / 'samples.jsonl').read_text().splitlines()]
        if len(samples) != 2 or {s['scenario'] for s in samples} != {'filter', 'count'}:
            raise RuntimeError('expected exactly two independent verified workload samples')
        for sample in samples:
            if (sample['engine'] != 'go' or sample['trial'] != 0 or
                    not sample['group_offsets_verified'] or
                    sample['result']['records'] != manifest['records'] or
                    sample['result']['nanos'] <= 0 or sample['peak_rss_kib'] <= 0 or
                    sample['observed_measurement_cpu_ns'] < 0 or
                    sample['batch_observation']['boundary_crossed'] or
                    sample['batch_observation']['measurement']['records'] != manifest['records'] or
                    sample['batch_observation']['warmup']['records'] != manifest['warmup'] or
                    sample['batch_observation']['measurement']['batches'] <= 0):
                raise RuntimeError('unverified/mixed sample; refusing configuration comparison')
            key = (sample['scenario'], round_id, budget)
            if key in by:
                raise RuntimeError('duplicate sample identity')
            by[key] = sample
            with (out / 'matrix-samples.jsonl').open('a') as output:
                output.write(json.dumps({'round': round_id, 'fetch_bytes': budget, 'sample': sample}) + '\n')

summary = {'diagnostic_only': True, 'instrumented': False, 'production_qualified': False,
           'scope': 'Ratios compare Go configurations within each round, not Go with Kafka Streams',
           'default_fetch_bytes_unchanged': 8 << 20, 'scenarios': {}}
for scenario in ['filter', 'count']:
    variants = {}
    for budget in budgets:
        rows = [by[scenario, r, budget] for r in range(args.rounds)]
        baseline = [by[scenario, r, 8 << 20] for r in range(args.rounds)]
        ratios = [base['result']['nanos'] / row['result']['nanos'] for base, row in zip(baseline, rows)]
        rss = [row['peak_rss_kib'] / base['peak_rss_kib'] for base, row in zip(baseline, rows)]
        cpu = [row['observed_measurement_cpu_ns'] / base['observed_measurement_cpu_ns']
               if base['observed_measurement_cpu_ns'] > 0 else None for base, row in zip(baseline, rows)]
        variants[str(budget)] = {
            'throughput_ratios_to_8mib_by_round': ratios,
            'median_throughput_ratio_to_8mib': statistics.median(ratios),
            'rss_ratios_to_8mib_by_round': rss,
            'median_rss_ratio_to_8mib': statistics.median(rss),
            'cpu_ratios_to_8mib_by_round': cpu,
            'median_records_per_second': statistics.median(manifest['records'] * 1e9 / row['result']['nanos'] for row in rows),
            'median_peak_rss_kib': statistics.median(row['peak_rss_kib'] for row in rows),
            'median_p99_latency_ms': statistics.median(row['result']['p99_latency_ns'] / 1e6 for row in rows),
            'median_observed_cpu_ns': statistics.median(row['observed_measurement_cpu_ns'] for row in rows),
            'median_records_per_batch': statistics.median(manifest['records'] / row['batch_observation']['measurement']['batches'] for row in rows)
        }
    summary['scenarios'][scenario] = variants
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
