# ADR 0002: Public Go API and generics

Status: Accepted for v0; exported API is experimental.

## Decision

Use StreamsBuilder plus KStream[K,V] and KTable[K,V]. Go methods cannot introduce
new type parameters. Consequently Stream[K,V], Map[K,V,K2,V2], Count[K,V],
WindowedCount[K,V], and Join[K,L,R,O] are package functions. Type-preserving
Filter, Process, and To are methods. Avoid reflection and any-valued records.
Record flags distinguish Kafka nulls from zero values. Codecs are explicit at
source, sink, store, and future exchange boundaries.

The typed Processor API is a factory returning a synchronous record handler.
Factories run once per task; closures own operator-local resources. Stateful
processors use the task's staged StoreGet/StorePut API. Arbitrary external side
effects inside callbacks cannot participate in transactions and are discouraged.
Records are immutable by contract: copying a Go struct does not clone slices or
pointers; callbacks must not retain or mutate borrowed data.

Each DSL operator registers a logical node and a typed handler factory. Compile
walks typed ports once per task, fusing execution through synchronous calls, not
through goroutines/channels. Node descriptions contain no function bodies. The
runtime sees byte records only at infrastructure boundaries. There is no
reflection or interface boxing between typed operators. Encoding and backend
interfaces do allocate; measure these costs instead of promising zero allocation.

## Alternatives

A Java-like fluent Map method cannot change its generic return type legally in
Go. An any-based graph simplifies construction but introduces assertions and
boxing for every record. Code generation complicates contributor workflows.
Top-level generic functions preserve compile-time checking and discoverability.

## Consequences

The public surface is intentionally small. Name sources, sinks and stores for
stable topology descriptions and changelog identities. Builders are single-owner
objects; Build freezes a snapshot and future mutations fail. Tasks instantiate
independent handlers and stores. API examples compile in the ordinary Go suite.
