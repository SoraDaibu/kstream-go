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
for scenario in ("filter", "filter-not", "mapvalues", "flatmap", "flatmapvalues", "merge", "count", "window", "table", "join"):
    rows = list(base)
    if scenario == "table": rows = [("table", ts, k, v) for _, ts, k, v in base]
    if scenario == "join":
        rows = [("table", 0, "a", "A"), ("in", 1, "a", "one"),
                ("table", 2, "a", "~"), ("in", 3, "a", "missing"),
                ("table", 4, "a", ""), ("in", 5, "a", "empty"),
                ("table", 6, "~", "ignored"), ("in", 7, "~", "ignored")]
    if scenario == "window":
        # Regression: Kafka 3.9.1 counts a null value when the key is present.
        rows = [("in", 1, "null-value", "~"), ("in", 2, "null-value", "x")] + rows
    if scenario == "merge":
        rows = [("in" if i % 2 == 0 else "right", ts, k, v) for i, (_, ts, k, v) in enumerate(rows)]
    rng = random.Random(73019)
    clock = 30
    for i in range(300):
        clock += rng.randrange(0, 4)
        timestamp = max(0, clock - rng.randrange(0, 25))
        key = rng.choice(["a", "b", "c", "", "~"])
        value = rng.choice(["one", "two", "", "~"])
        topic = "table" if scenario == "table" else "in"
        if scenario == "join": topic = rng.choice(["table", "in"])
        if scenario == "merge": topic = rng.choice(["in", "right"])
        rows.append((topic, timestamp, key, value))
    (out / (scenario + ".tsv")).write_text("".join("\t".join(map(str,r)) + "\n" for r in rows))
