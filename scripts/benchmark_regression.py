#!/usr/bin/env python3
"""Fail closed on missing measurements or per-case benchmark regressions."""
import argparse
import json
import math
import statistics
from pathlib import Path


METRICS = ('ns/op', 'B/op', 'allocs/op')
TIME_LIMIT = 1.05  # Existing clock-cache control budget; shared-host timing noise.
BYTE_LIMIT = 1.01  # Benchmark B/op is rounded and depends on calibration length.
MIN_PAIRS = 5


def validate(sample):
    if set(sample) != set(METRICS):
        raise ValueError('missing benchmark metrics')
    for metric, value in sample.items():
        if not math.isfinite(value) or value < 0 or (metric == 'ns/op' and value == 0):
            raise ValueError(f'invalid {metric}: {value}')


def parse_benchmarks(text):
    samples = {}
    for line in text.splitlines():
        if not line.startswith('Benchmark'):
            continue
        fields = line.split()
        if len(fields) < 8 or len(fields) % 2 or int(fields[1]) <= 0:
            raise ValueError(f'incomplete benchmark: {line}')
        name = fields[0]
        if name in samples:
            raise ValueError(f'duplicate benchmark: {name}')
        metrics = {}
        for number, unit in zip(fields[2::2], fields[3::2]):
            if unit in metrics:
                raise ValueError(f'duplicate metric: {line}')
            metrics[unit] = float(number)
        sample = {unit: metrics[unit] for unit in METRICS if unit in metrics}
        validate(sample)
        samples[name] = sample
    if not samples:
        raise ValueError('no benchmark measurements')
    return samples


def compare_trials(before, after):
    if len(before) < MIN_PAIRS or len(before) != len(after):
        raise ValueError(f'at least {MIN_PAIRS} complete paired trials required')
    names = set(before[0])
    if not names or any(set(trial) != names for trial in before + after):
        raise ValueError('benchmark coverage changed or a trial is incomplete')
    result = {'passed': True, 'production_qualified': False,
              'limits': {'time_ratio': TIME_LIMIT, 'bytes_ratio': BYTE_LIMIT,
                         'additional_allocs_per_op': 0},
              'cases': {}, 'failures': []}
    for name in sorted(names):
        baseline, candidate = [t[name] for t in before], [t[name] for t in after]
        for measurement in baseline + candidate:
            validate(measurement)
        ratios = [b['ns/op'] / a['ns/op'] for a, b in zip(baseline, candidate)]
        row = {'paired_time_ratios': ratios, 'time_ratio': statistics.median(ratios),
               'base': {m: statistics.median(s[m] for s in baseline) for m in METRICS},
               'head': {m: statistics.median(s[m] for s in candidate) for m in METRICS}}
        result['cases'][name] = row
        for metric, value, limit in [
            ('ns/op', row['time_ratio'], TIME_LIMIT),
            ('B/op', row['head']['B/op'], row['base']['B/op'] * BYTE_LIMIT),
            ('allocs/op', row['head']['allocs/op'], row['base']['allocs/op']),
        ]:
            if value > limit:
                result['failures'].append({'case': name, 'metric': metric,
                                           'value': value, 'limit': limit})
    result['passed'] = not result['failures']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.results / 'manifest.json').read_text())
    phases = {'base': [], 'head': []}
    for trial in range(manifest['pairs']):
        for phase, samples in phases.items():
            cases = {}
            for suite in manifest['suites']:
                text = (args.results / f'{trial}-{phase}-{suite}.txt').read_text()
                cases.update({f'{suite}/{k}': v for k, v in parse_benchmarks(text).items()})
            samples.append(cases)
    result = compare_trials(phases['base'], phases['head'])
    (args.results / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
