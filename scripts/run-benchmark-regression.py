#!/usr/bin/env python3
"""Compare immutable revisions using the baseline's benchmark code on one CPU."""
import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from benchmark_regression import MIN_PAIRS


SUITES = {
    'record': '^BenchmarkBytesClone$',
    'execution': '^Benchmark(TransactionBatch|RecordMetadataCapture)$',
    'stream': '^Benchmark(ProcessingClockDispatch|TypedProcessorDispatch|ProcessingClockSetup|WindowExpiry|WindowReduce|BranchRouting)$',
}


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True, help='PR base commit or push before commit')
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--output', default='results/regression', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('use an empty output directory to retain each run separately')
    commits = {phase: run(['git', 'rev-parse', '--verify', '--end-of-options', ref + '^{commit}'],
                          cwd=root, capture_output=True, text=True).stdout.strip()
               for phase, ref in [('base', args.base), ('head', args.head)]}
    environment = dict(os.environ, GOMAXPROCS='1', GOMEMLIMIT='512MiB', GOTOOLCHAIN='local')
    cpu = min(os.sched_getaffinity(0))
    go_version = run(['go', 'version'], env=environment, capture_output=True, text=True).stdout.strip()
    manifest = {'commits': commits, 'go_version': go_version, 'host': platform.platform(),
                'cpu': cpu, 'gomaxprocs': 1, 'gomemlimit': '512MiB',
                'pairs': MIN_PAIRS, 'benchtime': '1s', 'suites': SUITES,
                'harness': 'baseline test files for both binaries',
                'production_qualified': False, 'benchmark_sha256': {}, 'binary_sha256': {},
                'order': [['base', 'head'] if i % 2 == 0 else ['head', 'base'] for i in range(MIN_PAIRS)]}
    (output / 'environment.txt').write_text(
        go_version + '\n' + run(['lscpu'], capture_output=True, text=True).stdout)
    # Isolated snapshots avoid changing either the user's checkout or Git metadata.
    with tempfile.TemporaryDirectory(prefix='kstream-regression-') as temporary:
        work = Path(temporary)
        sources = {}
        for phase, commit in commits.items():
            sources[phase] = work / phase
            sources[phase].mkdir()
            archive = work / f'{phase}.tar'
            with archive.open('wb') as target:
                run(['git', 'archive', '--format=tar', commit], cwd=root, stdout=target)
            with tarfile.open(archive) as content:
                content.extractall(sources[phase], filter='data')
        # A candidate cannot improve its result by deleting or weakening a benchmark.
        # Include all baseline test helpers; ordinary CI still tests the candidate's tests.
        for suite in SUITES:
            for path in (sources['head'] / suite).glob('*_test.go'):
                path.unlink()
            for path in (sources['base'] / suite).glob('*_test.go'):
                destination = sources['head'] / suite / path.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
                manifest['benchmark_sha256'][f'{suite}/{path.name}'] = sha256(path)
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        binaries = {}
        for phase in commits:
            for suite in SUITES:
                binary = work / f'{phase}-{suite}.test'
                with (output / f'{phase}-{suite}-build.txt').open('w') as log:
                    run(['go', 'test', '-c', '-o', str(binary), './' + suite],
                        cwd=sources[phase], env=environment, stdout=log, stderr=subprocess.STDOUT)
                binaries[phase, suite] = binary
                manifest['binary_sha256'][f'{phase}/{suite}'] = sha256(binary)
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        for trial, order in enumerate(manifest['order']):
            for phase in order:
                for suite, pattern in SUITES.items():
                    print(f'pair {trial + 1}/{MIN_PAIRS}: {phase}/{suite}', flush=True)
                    with (output / f'{trial}-{phase}-{suite}.txt').open('w') as log:
                        run(['taskset', '-c', str(cpu), str(binaries[phase, suite]),
                             '-test.run=^$', '-test.bench=' + pattern, '-test.benchmem',
                             '-test.benchtime=1s', '-test.count=1', '-test.cpu=1'],
                            cwd=sources[phase], env=environment, stdout=log, stderr=subprocess.STDOUT,
                            timeout=300)
    run(['python3', str(root / 'scripts/benchmark_regression.py'), str(output)])


if __name__ == '__main__':
    main()
