# ADR 0012: Kafka group transactions through a bounded worker owner

Status: Accepted for the experimental eager-range worker after focused broker restart/rebalance/kill validation.

Use franz-go's public GroupTransactSession API to coordinate Kafka group offsets,
changelogs and outputs. Do not duplicate the client's protocol implementation.
One RunGroup invocation owns one consumer/producer session and serially schedules
its assigned partition tasks. Poll record count and restore batches are explicit
bounds. There are no goroutines/channels per operator or task by default.

Application ID is the consumer group ID. An explicit stable unique instance ID
determines the worker transactional ID and fences a duplicate worker identity.
Before any input poll or task restoration, initialize that producer epoch with
an aborted internal control-record transaction. This fences an abandoned worker
transaction without committing any group offsets, so read-committed task restore
cannot wait behind the previous incarnation until its transaction timeout.
Call the client's public ProducerID API before producing that control record and
retry explicitly retryable initialization errors within the initialization budget.
The forced-kill test exposed CONCURRENT_TRANSACTIONS while the broker aborts the
old epoch. Sending a record before initialization finished let the client
classify that transient startup condition as a fatal producer error. Only startup
initialization is retried; transaction-end errors remain fatal and ambiguous.
Every group participant must run the same topology and subscribe to the same
source topics. Initially require equal source partition counts and the eager
range assignor, which co-locates corresponding partitions when every member
subscribes to those topics. Validate co-location on assignment and provisioned
changelog capacity before processing. Partition counts must remain fixed while
running. Cooperative assignment, heterogeneous subtopologies and automatic
exchanges remain separate work.

Callbacks publish immutable assignment snapshots behind infrastructure-only
synchronization. They do not close or mutate task stores. Poll with a bounded
wait so the worker can release old task leases even when idle. Rebuild all owned
task incarnations after assignment changes or an aborted rebalance transaction.
The worker's processing owner performs restore, callbacks, transaction and cache
publication. Long-running callbacks and restore must fit configured group/poll
timeouts; preempting arbitrary Go callbacks is not attempted.

Restore committed state using the existing fenced, unique-barrier protocol,
closing temporary task restore clients afterward. Commit data using the worker
session's transaction, not the temporary per-task producer. Group generation
metadata protects against an old group owner committing. Require stable offset
fetches and read-committed input. Mixed fixed-assignment and group deployments
sharing an application identity are unsupported.

Before committing, validate task generations, assignment revision and coverage
of every polled topic/partition through proposed checkpoints. Build complete
per-task checkpoints and produce all state/output/checkpoint records, then end
the transaction with the consumer-group offsets tracked by the session. A
rebalance-aborted decision poisons/discards local task incarnations and retries
from committed positions. Other errors return to the caller after cleanup;
ambiguous commit outcomes are never retried in place. Apply local caches only
after a successful broker decision via execution.Group.

This is an experimental first group worker. Test real-broker committed offsets,
restart/replay, several tasks in one transaction, and a second worker joining
while the first is blocked before commit. Add forced process kill, ambiguous
network failure, transaction timeout, compaction, standby and broader lifecycle
tests before claiming full Kafka Streams fault tolerance or qualifying throughput.
