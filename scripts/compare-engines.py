#!/usr/bin/env python3
"""Paired exploratory driver measurements. Always reports production unqualified."""
import csv
import json
import os
import random
import statistics
import subprocess
import sys
from pathlib import Path

binary, classpath, directory = sys.argv[1:]
out = Path(directory); out.mkdir(parents=True, exist_ok=True)
records = int(os.getenv('KSTREAM_COMPARE_RECORDS', '200000'))
warmup = int(os.getenv('KSTREAM_COMPARE_WARMUP', '100000'))
trials = int(os.getenv('KSTREAM_COMPARE_TRIALS', '5'))
if records < 1 or warmup < 0 or trials < 5:
    raise SystemExit('positive records, nonnegative warmup and >=5 trials required')
manifest = {'reference': 'Kafka Streams 3.9.1 TopologyTestDriver', 'store': 'in-memory',
            'durability': 'none', 'input_keys': 64, 'value': 'value', 'records': records,
            'warmup_records': warmup, 'trials': trials, 'cpus': 1, 'transaction_batch': 1,
            'production_qualified': False,
            'limitation': 'Different in-process drivers; no brokers, persistent stores, EOS or tail-latency qualification.'}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
env = dict(os.environ, GOMAXPROCS='1', GOMEMLIMIT='512MiB')
commands = {'go': [binary], 'kafka': ['java', '-XX:ActiveProcessorCount=1', '-Xms32m', '-Xmx512m', '-cp', classpath, 'dev.kstream.EngineBenchmark']}
samples = []
with (out / 'samples.csv').open('w') as f:
    writer = csv.writer(f); writer.writerow(['engine','scenario','records','nanos','checksum','rss_kib','trial'])
    for scenario in ['filter', 'count']:
        for trial in range(trials):
            checksums = []
            order = ['go','kafka'] if trial % 2 == 0 else ['kafka','go']
            for engine in order:
                result = subprocess.run(commands[engine] + [scenario, str(records), str(warmup)], env=env, text=True, capture_output=True)
                if result.returncode:
                    print(result.stdout, file=sys.stderr)
                    print(result.stderr, file=sys.stderr)
                    raise SystemExit(result.returncode)
                row = next(csv.reader([result.stdout.strip()]))
                if len(row) != 6 or row[0] != engine or row[1] != scenario or int(row[2]) != records:
                    raise RuntimeError('invalid engine output: ' + result.stdout)
                checksums.append(row[4]); writer.writerow(row + [trial]); f.flush()
                samples.append(dict(engine=engine,scenario=scenario,trial=trial,nanos=int(row[3]),rss_kib=int(row[5])))
            if len(set(checksums)) != 1:
                raise RuntimeError('output checksum mismatch for ' + scenario)
summary = {'production_qualified': False, 'target_ratio': 2.5, 'scenarios': {}}
rng = random.Random(73019)
for scenario in ['filter','count']:
    by = {(r['engine'],r['trial']):r for r in samples if r['scenario']==scenario}
    ratios = [by['kafka',i]['nanos']/by['go',i]['nanos'] for i in range(trials)]
    bootstrap = sorted(statistics.median(rng.choices(ratios,k=trials)) for _ in range(10000))
    summary['scenarios'][scenario] = {'median_paired_throughput_ratio': statistics.median(ratios),
        'exploratory_bootstrap_95pct': [bootstrap[250],bootstrap[9749]],
        'go_median_rss_kib': statistics.median(by['go',i]['rss_kib'] for i in range(trials)),
        'kafka_median_rss_kib': statistics.median(by['kafka',i]['rss_kib'] for i in range(trials))}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
