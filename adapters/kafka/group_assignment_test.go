package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
)

func TestGroupAssignmentCoLocationAndRevocation(t *testing.T) {
	tracker := &assignmentTracker{topics: []string{"left", "right"}}
	tracker.assigned(context.Background(), nil, map[string][]int32{"left": {0, 2}, "right": {2, 0}})
	snapshot := tracker.current.Load()
	if snapshot.err != nil || !snapshot.valid || len(snapshot.partitions) != 2 {
		t.Fatal("valid co-partitioned assignment rejected", snapshot)
	}
	tracker.revoked(context.Background(), nil, nil)
	if tracker.current.Load().valid || !snapshot.valid || len(snapshot.partitions) != 2 {
		t.Fatal("revocation mutated previous immutable assignment")
	}
	for _, bad := range []map[string][]int32{{"left": {0}, "right": {1}}, {"left": {0, 0}, "right": {0}}, {"left": {-1}, "right": {-1}}, {"unknown": {0}}} {
		tracker.assigned(context.Background(), nil, bad)
		if tracker.current.Load().err == nil {
			t.Fatal("unsafe assignment accepted", bad)
		}
	}
}
func TestGroupConfigBoundsAndIdentity(t *testing.T) {
	valid := GroupConfig{Kafka: Config{Brokers: []string{"unused:9092"}, ChangelogTopic: "state"}, ApplicationID: "app", InstanceID: "worker", Topics: []string{"in"}}
	for _, mode := range []string{"application", "instance", "topic", "duplicate", "changelog", "batch", "fetch", "poll", "restore"} {
		config := valid
		switch mode {
		case "application":
			config.ApplicationID = ""
		case "instance":
			config.InstanceID = ""
		case "topic":
			config.Topics = nil
		case "duplicate":
			config.Topics = []string{"in", "in"}
		case "changelog":
			config.Topics = []string{"state"}
		case "batch":
			config.BatchSize = -1
		case "fetch":
			config.FetchBytes = 1
		case "poll":
			config.PollInterval = -time.Second
		case "restore":
			config.RestoreTimeout = -time.Second
		}
		if _, err := normalizeGroup(config); err == nil {
			t.Fatal("invalid group config accepted", mode)
		}
	}
	config, err := normalizeGroup(valid)
	if err != nil || config.BatchSize != 256 || config.FetchBytes != 8<<20 || config.PollInterval != 250*time.Millisecond {
		t.Fatal(config, err)
	}
	if workerTransactionID("a", "b") == workerTransactionID("a", "c") || workerTransactionID("a", "b") == transactionID(execution.Spec{ApplicationID: "a", ID: "b"}) {
		t.Fatal("worker identity collides")
	}
}
