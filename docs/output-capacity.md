# Transaction output capacity

By default, output record headers grow with Go append. For a known emission
rate, execution.WithOutputCapacity(n) supplies an initial capacity hint:

```go
task, err := plan.NewTask(ctx, spec, backend,
    execution.WithStoreFactory(engine.Task),
    execution.WithOutputCapacity(4096))
```

Valid hints are 0 through 65,536. Zero preserves ordinary growth. Allocation
happens on the first output in each transaction, so transactions without output
reserve no output array. Capacity is an initial reservation, not an output limit;
fanout may grow beyond it. Each transaction uses a fresh array, preserving the
lifetime of retained backend records and cloned payloads.

Choose the hint for expected output count per transaction, rather than input
batch size alone. A selective filter can emit far fewer records; an oversized
hint allocates unused headers. Fanout can require more than one output per input.
The retained benchmark includes dense, sparse, zero-output, fanout and changing
emission rates, including intentionally mismatched hints. The library default
does not infer cardinality or change between batches.

The broker experiment explicitly reserves 4,096 headers for its known
one-output-per-input filter/count fixtures, and records this in the manifest.
The script default remains zero; set KSTREAM_BROKER_OUTPUT_CAPACITY=4096 to run
the reserved-capacity experiment. Output correctness, store/changelog decisions
and committed group-offset checks remain identical. Full production performance
qualification and feature parity remain open.
