#!/usr/bin/env python3
"""Deterministic fixtures plus seeded late/null/update traces; no third-party deps."""
import json
import random
import sys
from pathlib import Path
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
base = [("in", 0, "a", "one"), ("in", 9, "a", "two"), ("in", 10, "b", "x"),
        ("in", 14, "a", "x"), ("in", 8, "a", "late"), ("in", 15, "b", "clock"),
        ("in", 9, "a", "closed"), ("in", 20, "~", "null-key"),
        ("in", 21, "a", "~"), ("in", 22, "", "")]
window_folds = ("window-reduce", "hopping-reduce", "hopping-uneven-reduce", "window-aggregate", "window-aggregate-null", "hopping-aggregate", "hopping-aggregate-null", "hopping-uneven-aggregate", "hopping-uneven-aggregate-null")
for scenario in ("filter", "filter-not", "branch", "branch-default", "mapvalues", "flatmap", "flatmapvalues", "merge", "repartition-values", "repartition-count", "clock-disconnected", "clock-repartition", "metadata", "metadata-fanout", "process-typed", "process-typed-fanout", "count", "reduce", "aggregate", "aggregate-null", "window", "hopping", "hopping-uneven", "window-reduce", "hopping-reduce", "hopping-uneven-reduce", "window-aggregate", "window-aggregate-null", "hopping-aggregate", "hopping-aggregate-null", "hopping-uneven-aggregate", "hopping-uneven-aggregate-null", "table", "table-filter", "table-filter-not", "table-mapvalues", "table-transform-join", "join", "join-null", "left-join"):
    rows = list(base)
    if scenario.startswith("process-typed"):
        rows += [("in", 3, "é🌊", "漢字"), ("in", 4, "skip", "!skip"),
                 ("in", 2, "after-skip", ""), ("in", 1, "~", "~")]
    if scenario in ("reduce", "aggregate", "aggregate-null"):
        # Deletion/reinitialization must reset the aggregate timestamp lifetime.
        # Reduce uses a first value directly, even if the reducer would delete it.
        rows = [("in", 100, "reset", "first"), ("in", 20, "reset", "!delete"),
                ("in", 5, "reset", "new"), ("in", 2, "reset", "old"),
                ("in", 110, "absent", "!delete"), ("in", 1, "absent", "!delete"),
                ("in", 2, "absent", "")] + rows
    if scenario == "table" or scenario.startswith("table-"):
        rows = [("table", ts, k, v) for _, ts, k, v in base]
        if scenario != "table":
            rows += [("table", 3, "absent", "~"), ("table", 4, "rejected", ""),
                     ("table", 5, "rejected", ""), ("table", 6, "rejected", "~"),
                     ("table", 2, "a", "!null-output")]
        if scenario in ("table-filter", "table-filter-not"):
            fixture = Path(__file__).resolve().parent.parent / "integration/fixtures/table-filter-noop-delete.json"
            rows = [tuple("~" if cell is None else cell for cell in row) for row in json.loads(fixture.read_text())] + rows
    if scenario in ("join", "join-null", "left-join", "table-transform-join"):
        rows = [("table", 0, "a", "A"), ("in", 1, "a", "one"),
                ("table", 2, "a", "~"), ("in", 3, "a", "missing"),
                ("table", 4, "a", ""), ("in", 5, "a", "empty"),
                ("table", 6, "~", "ignored"), ("in", 7, "~", "ignored")]
        if scenario != "join":
            rows += [("in", 8, "a", "!null-output"), ("in", 9, "missing", "!null-output"),
                     ("in", 10, "a", "~"), ("table", 11, "", ""), ("in", 12, "", "empty-key")]
    if scenario in window_folds:
        rows = [("in", 9, "reset", "first"), ("in", 4, "reset", "second"),
                ("in", 3, "reset", "!delete"), ("in", 2, "reset", "!delete"),
                ("in", 5, "reset", ""), ("in", 6, "reset", "~"),
                ("in", 6, "reset", "next"),
                ("in", 9, "boundary", "first"), ("in", 15, "clock", "x"),
                ("in", 9, "boundary", "overlap"), ("in", 9, "boundary", "!delete"),
                ("in", 8, "boundary", "new"), ("in", 20, "clock", "x"),
                ("in", 9, "boundary", "closed")] + rows
    if scenario in ("window", "hopping", "hopping-uneven"):
        # Regression: Kafka 3.9.1 counts a null value when the key is present.
        rows = [("in", 1, "null-value", "~"), ("in", 2, "null-value", "x")] + rows
        if scenario != "window":
            rows += [("in", 15, "clock", "x"), ("in", 9, "boundary", "x"),
                     ("in", 20, "clock", "x"), ("in", 14, "boundary", "~"),
                     ("in", 1000, "~", "ignored"), ("in", 19, "boundary", "x")]
    if scenario in ("merge", "clock-disconnected", "metadata", "metadata-fanout", "process-typed", "process-typed-fanout"):
        rows = [("in" if i % 2 == 0 else "right", ts, k, v) for i, (_, ts, k, v) in enumerate(rows)]
    rng = random.Random(73019)
    clock = 30
    for i in range(300):
        clock += rng.randrange(0, 4)
        timestamp = max(0, clock - rng.randrange(0, 25))
        key = rng.choice(["a", "b", "c", "", "~"])
        value = rng.choice(["one", "two", "", "~"])
        if scenario in ("reduce", "aggregate", "aggregate-null") or scenario in window_folds:
            value = rng.choice(["one", "two", "", "~", "!delete"])
        topic = "table" if scenario == "table" or scenario.startswith("table-") else "in"
        if scenario in ("join", "join-null", "left-join", "table-transform-join"): topic = rng.choice(["table", "in"])
        if scenario in ("join-null", "left-join", "table-mapvalues", "table-transform-join") and rng.randrange(5) == 0: value = "!null-output"
        if scenario in ("merge", "clock-disconnected", "metadata", "metadata-fanout", "process-typed", "process-typed-fanout"): topic = rng.choice(["in", "right"])
        rows.append((topic, timestamp, key, value))
    if scenario.startswith("clock-"):
        fixture = Path(__file__).resolve().parent.parent / "integration/fixtures/clock-driver-task-isolation.json"
        prefix = [tuple("~" if cell is None else cell for cell in row) for row in json.loads(fixture.read_text())]
        if scenario == "clock-repartition": prefix = [("in", ts, key, value) for _, ts, key, value in prefix]
        rows = prefix + rows
    if scenario in window_folds:
        fixture = Path(__file__).resolve().parent.parent / "integration/fixtures/window-reduce-null-input.json"
        rows += [tuple("~" if cell is None else cell for cell in row) for row in json.loads(fixture.read_text())]
    (out / (scenario + ".tsv")).write_text("".join("\t".join(map(str,r)) + "\n" for r in rows))
