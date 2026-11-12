import importlib.util
import json
import math
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    'gate', Path(__file__).parents[1] / 'benchmark_regression.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def sample(ns=100, size=64, allocs=2):
    return {'ns/op': ns, 'B/op': size, 'allocs/op': allocs}


def trials(**cases):
    return [dict(cases) for _ in range(5)]


class BenchmarkRegressionTests(unittest.TestCase):
    def test_parse_real_go_output_with_extra_metrics(self):
        result = gate.parse_benchmarks('''goos: linux
goarch: amd64
BenchmarkCopy/key=8-1   10000   1.2e+02 ns/op   5.00 MB/s   64 B/op   2 allocs/op
BenchmarkBatch   50   250 ns/op   64 B/op   2 allocs/op   1024 records/op
PASS
''')
        self.assertEqual(result['BenchmarkCopy/key=8-1'], sample(120))
        self.assertEqual(result['BenchmarkBatch'], sample(250))

    def test_parse_rejects_empty_duplicate_incomplete_or_invalid_samples(self):
        valid = 'BenchmarkCopy 10 100 ns/op 64 B/op 2 allocs/op\n'
        for text in ['', 'PASS\n', valid * 2,
                     'BenchmarkCopy 10 100 ns/op\n',
                     valid.replace('10 100', '0 100'),
                     valid.replace('100 ns', 'nan ns'),
                     valid.replace('100 ns', 'inf ns'),
                     valid.replace('100 ns', '0 ns'),
                     valid.replace('64 B', '-1 B')]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                gate.parse_benchmarks(text)

    def test_unchanged_and_improved_results_pass_without_qualifying(self):
        for after in [sample(), sample(50, 0, 0)]:
            result = gate.compare_trials(trials(copy=sample()), trials(copy=after))
            self.assertTrue(result['passed'])
            self.assertFalse(result['production_qualified'])

    def test_each_case_and_metric_must_pass(self):
        for metric, after in [('ns/op', sample(105.01)),
                              ('B/op', sample(size=65)),
                              ('allocs/op', sample(allocs=3))]:
            with self.subTest(metric=metric):
                result = gate.compare_trials(
                    trials(slow=sample(), fast=sample()),
                    trials(slow=after, fast=sample(1, 0, 0)))
                self.assertFalse(result['passed'])
                self.assertEqual(result['failures'][0]['case'], 'slow')
                self.assertEqual(result['failures'][0]['metric'], metric)

    def test_boundaries_are_not_rounded_into_a_pass(self):
        self.assertTrue(gate.compare_trials(trials(a=sample()), trials(a=sample(105)))['passed'])
        self.assertFalse(gate.compare_trials(trials(a=sample()), trials(a=sample(105.00001)))['passed'])

    def test_zero_allocation_baseline_cannot_increase(self):
        for after in [sample(size=1, allocs=0), sample(size=0, allocs=1)]:
            self.assertFalse(gate.compare_trials(
                trials(a=sample(size=0, allocs=0)), trials(a=after))['passed'])

    def test_paired_ratios_and_outlier_resistance(self):
        before = [{'a': sample(n)} for n in [100, 200, 400, 100, 200]]
        after = [{'a': sample(n)} for n in [100, 200, 400, 100, 20000]]
        result = gate.compare_trials(before, after)
        self.assertTrue(result['passed'])
        self.assertEqual(result['cases']['a']['time_ratio'], 1)
        self.assertEqual(result['cases']['a']['paired_time_ratios'], [1, 1, 1, 1, 100])

    def test_missing_new_duplicate_or_partial_trial_cannot_pass(self):
        valid = trials(a=sample())
        for before, after in [(valid[:4], valid[:4]), (valid, valid[:4]),
                              (valid, trials()), (trials(), trials()),
                              (valid, trials(b=sample())),
                              (valid, valid[:4] + [{'a': sample(), 'b': sample()}])]:
            with self.subTest(before=before, after=after), self.assertRaises(ValueError):
                gate.compare_trials(before, after)

    def test_invalid_numeric_data_cannot_bypass_gate(self):
        for value in [math.nan, math.inf, -1, 0]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                gate.compare_trials(trials(a=sample()), trials(a=sample(value)))

    def test_cli_exit_status_and_retained_decision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'manifest.json').write_text(json.dumps({'pairs': 5, 'suites': ['record']}))
            for after_ns, code in [(100, 0), (106, 1)]:
                for trial in range(5):
                    for phase, ns in [('base', 100), ('head', after_ns)]:
                        (root / f'{trial}-{phase}-record.txt').write_text(
                            f'BenchmarkCopy 10 {ns} ns/op 64 B/op 2 allocs/op\nPASS\n')
                result = subprocess.run([sys.executable, gate.__file__, str(root)],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stderr)
                decision = json.loads((root / 'summary.json').read_text())
                self.assertEqual(decision['passed'], code == 0)
                self.assertFalse(decision['production_qualified'])
            (root / '4-head-record.txt').unlink()
            result = subprocess.run([sys.executable, gate.__file__, str(root)],
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)

    def test_rejects_the_retained_clock_cache_regression(self):
        root = (Path(__file__).parents[2] / 'docs/benchmarks/'
                '2026-10-24-clock-cache/private')
        phases = {'before': [], 'after': []}
        for trial in range(1, 6):
            for phase, samples in phases.items():
                samples.append(gate.parse_benchmarks(
                    (root / f'{trial}-{phase}-controls.txt').read_text()))
        result = gate.compare_trials(phases['before'], phases['after'])
        self.assertFalse(result['passed'])
        self.assertTrue(any(f['metric'] == 'ns/op' and
                            f['case'].startswith('BenchmarkWindowExpiry/keys=4096/size=1152921504606846976')
                            for f in result['failures']))


if __name__ == '__main__':
    unittest.main()
