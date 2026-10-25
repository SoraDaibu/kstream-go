#!/usr/bin/env python3
"""Evaluate the predeclared local clock-cache criteria from all paired samples."""
import json
import re
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "private"
manifest = json.loads((root / "manifest.json").read_text())
criteria = manifest["criteria"]
samples = {}
pattern = re.compile(r"(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op.*?\s+(\d+) B/op\s+(\d+) allocs/op")
for path in sorted(root.glob("[1-5]-*-*.txt")):
    trial, phase, _ = path.stem.split("-")
    for line in path.read_text().splitlines():
        match = pattern.match(line)
        if match:
            key = (int(trial), phase)
            case = samples.setdefault(match[1], {})
            if key in case:
                raise ValueError(f"duplicate sample: {path}: {match[1]}")
            case[key] = tuple(map(float, match.groups()[1:]))
if len(samples) != 24:
    raise ValueError(f"expected 24 cases, found {len(samples)}")

failures, results = [], []
for name, values in sorted(samples.items()):
    pairs = [(values[(i, "before")], values[(i, "after")]) for i in range(1, manifest["pairs"] + 1)]
    ratios = [after[0] / before[0] for before, after in pairs]
    median = statistics.median(ratios)
    limit = criteria["ordinary_dispatch_window_and_pipeline_median_after_before_max"]
    if name == "BenchmarkProcessingClockDispatch/batch=256/scoped=true":
        limit = criteria["scoped_batch_256_median_after_before_max"]
    elif name.startswith("BenchmarkProcessingClockSetup"):
        limit = criteria["scope_setup_bytes_and_time_median_after_before_max"]
        byte_ratio = statistics.median([after[1] / before[1] for before, after in pairs])
        if byte_ratio > limit:
            failures.append({"case": name, "metric": "setup_bytes", "ratio": byte_ratio, "limit": limit})
    if median > limit:
        failures.append({"case": name, "metric": "time", "ratio": median, "limit": limit})
    if name.startswith(("BenchmarkProcessingClockDispatch", "BenchmarkTypedProcessorDispatch")):
        if any(before[1:] != after[1:] for before, after in pairs):
            failures.append({"case": name, "metric": "transaction_allocation_changed"})
    results.append({
        "case": name,
        "median_time_ratio": median,
        "min_time_ratio": min(ratios),
        "max_time_ratio": max(ratios),
        "median_before_ns": statistics.median([before[0] for before, _ in pairs]),
        "median_after_ns": statistics.median([after[0] for _, after in pairs]),
        "median_before_bytes": statistics.median([before[1] for before, _ in pairs]),
        "median_after_bytes": statistics.median([after[1] for _, after in pairs]),
        "median_before_allocations": statistics.median([before[2] for before, _ in pairs]),
        "median_after_allocations": statistics.median([after[2] for _, after in pairs]),
    })
print(json.dumps({"criteria_passed": not failures, "cases": results, "failures": failures}, indent=2))
sys.exit(1 if failures else 0)
