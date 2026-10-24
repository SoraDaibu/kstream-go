package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestGroupClockCheckpointOwnershipAndAtomicDecision(t *testing.T) {
	for _, mode := range []string{"commit", "abort", "error", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			b, session, transactions := groupBackendFixture()
			for p, entry := range b.entries {
				entry.checkpoint.ScopeTimes = map[string]int64{"left": 1, "right": 0}
				b.entries[p] = entry
			}
			for i := range transactions {
				if mode != "legacy" {
					transactions[i].Transaction.ScopeTimes = map[string]int64{"left": 5, "right": 0}
				}
			}
			if mode == "abort" {
				session.committed = false
			}
			if mode == "error" {
				session.commitErr = errors.New("ambiguous decision")
			}
			err := b.CommitTasks(context.Background(), transactions)
			if (mode == "commit" || mode == "legacy") != (err == nil) {
				t.Fatal(mode, err)
			}
			want := int64(1)
			if mode == "commit" {
				want = 5
			}
			for _, entry := range b.entries {
				if !reflect.DeepEqual(entry.checkpoint.ScopeTimes, map[string]int64{"left": want, "right": 0}) {
					t.Fatal("clock escaped durable decision", entry.checkpoint)
				}
			}
			if mode == "commit" {
				transactions[0].Transaction.ScopeTimes["left"] = 999
				if b.entries[0].checkpoint.ScopeTimes["left"] != 5 {
					t.Fatal("backend retained caller clock map")
				}
			}
			for _, row := range session.rows {
				key, err := decodeKey(row.Key)
				if err != nil || key.kind != metadataKind {
					continue
				}
				var meta checkpoint
				if err := json.Unmarshal(row.Value, &meta); err != nil {
					t.Fatal(err)
				}
				staged := int64(5)
				if mode == "legacy" {
					staged = 1
				}
				if !reflect.DeepEqual(meta.ScopeTimes, map[string]int64{"left": staged, "right": 0}) {
					t.Fatal("wire checkpoint lost complete processing clocks", meta)
				}
			}
		})
	}
}

func TestLegacyCheckpointClockFieldIsOmitted(t *testing.T) {
	encoded, err := json.Marshal(checkpoint{NextOffsets: map[string]int64{"in": 2}, StreamTime: 9})
	if err != nil || string(encoded) != `{"next_offsets":{"in":2},"stream_time":9}` {
		t.Fatal("ordinary checkpoint wire format changed", string(encoded), err)
	}
	var meta checkpoint
	if err := json.Unmarshal(encoded, &meta); err != nil || meta.ScopeTimes != nil {
		t.Fatal("legacy checkpoint did not restore", meta, err)
	}
}
