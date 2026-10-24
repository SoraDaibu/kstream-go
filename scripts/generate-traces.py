#!/usr/bin/env python3
"""Deterministic fixtures plus seeded late/null/update traces; no third-party deps."""
import random
import sys
from pathlib import Path
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
base = [("in", 0, "a", "one"), ("in", 9, "a", "two"), ("in", 10, "b", "x"),
        ("in", 14, "a", "x"), ("in", 8, "a", "late"), ("in", 15, "b", "clock"),
        ("in", 9, "a", "closed"), ("in", 20, "~", "null-key"),
        ("in", 21, "a", "~"), ("in", 22, "", "")]
for scenario in ("filter", "filter-not", "mapvalues", "flatmap", "flatmapvalues", "merge", "count", "reduce", "aggregate", "aggregate-null", "window", "hopping", "hopping-uneven", "table", "table-filter", "table-filter-not", "table-mapvalues", "table-transform-join", "join", "join-null", "left-join"):
    rows = list(base)
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
    if scenario in ("join", "join-null", "left-join", "table-transform-join"):
        rows = [("table", 0, "a", "A"), ("in", 1, "a", "one"),
                ("table", 2, "a", "~"), ("in", 3, "a", "missing"),
                ("table", 4, "a", ""), ("in", 5, "a", "empty"),
                ("table", 6, "~", "ignored"), ("in", 7, "~", "ignored")]
        if scenario != "join":
            rows += [("in", 8, "a", "!null-output"), ("in", 9, "missing", "!null-output"),
                     ("in", 10, "a", "~"), ("table", 11, "", ""), ("in", 12, "", "empty-key")]
    if scenario in ("window", "hopping", "hopping-uneven"):
        # Regression: Kafka 3.9.1 counts a null value when the key is present.
        rows = [("in", 1, "null-value", "~"), ("in", 2, "null-value", "x")] + rows
        if scenario != "window":
            rows += [("in", 15, "clock", "x"), ("in", 9, "boundary", "x"),
                     ("in", 20, "clock", "x"), ("in", 14, "boundary", "~"),
                     ("in", 1000, "~", "ignored"), ("in", 19, "boundary", "x")]
    if scenario == "merge":
        rows = [("in" if i % 2 == 0 else "right", ts, k, v) for i, (_, ts, k, v) in enumerate(rows)]
    rng = random.Random(73019)
    clock = 30
    for i in range(300):
        clock += rng.randrange(0, 4)
        timestamp = max(0, clock - rng.randrange(0, 25))
        key = rng.choice(["a", "b", "c", "", "~"])
        value = rng.choice(["one", "two", "", "~"])
        if scenario in ("reduce", "aggregate", "aggregate-null"):
            value = rng.choice(["one", "two", "", "~", "!delete"])
        topic = "table" if scenario == "table" or scenario.startswith("table-") else "in"
        if scenario in ("join", "join-null", "left-join", "table-transform-join"): topic = rng.choice(["table", "in"])
        if scenario in ("join-null", "left-join", "table-mapvalues", "table-transform-join") and rng.randrange(5) == 0: value = "!null-output"
        if scenario == "merge": topic = rng.choice(["in", "right"])
        rows.append((topic, timestamp, key, value))
    (out / (scenario + ".tsv")).write_text("".join("\t".join(map(str,r)) + "\n" for r in rows))
