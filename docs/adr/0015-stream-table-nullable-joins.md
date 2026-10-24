# ADR 0015: Stream-table left joins and nullable results

Status: Accepted for current-value lookup; independent oracle and broker gates passed.

Keep Join as the existing non-null-result inner-join convenience. Add JoinNullable
for inner joiners that can emit Kafka null values, and LeftJoin with an explicit
right-null flag and nullable result. Go zero values remain ordinary data. These
are package functions because output types may differ from either input type.

Only stream arrivals emit results. Null-valued stream input is skipped. Inner
joins skip null keys and absent table entries. Left joins preserve null stream
keys and call the joiner with a null right value without looking up a store key,
matching the Kafka Streams 3.9.1 reference's null-key left-join behavior. Present
empty/zero table values have rightNull=false. A null result forwards a tombstone
record with the stream key/timestamp, rather than suppressing it.

Use one typed synchronous table lookup implementation and preserve task-local
state ownership. Inputs must share a builder and partitioned keys; key changes
still require a real exchange. This is current-value nonversioned lookup, not
as-of/versioned or foreign-key joining. Cross-input runtime scheduling remains
a separate compatibility gate.

Validate fixed and seeded traces for null/missing/empty inputs, deletion, nullable
results and timestamps against Kafka Streams 3.9.1 using both store backends,
three batch sizes and independent/group commits. Keep focused recovery and null
flag tests, and reject nil joiners or unpartitioned key changes during Build.
