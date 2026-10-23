package kafka

import (
	"bytes"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
)

func TestChangelogKeyScopeAndBinaryFields(t *testing.T) {
	spec := execution.Spec{ApplicationID: "app\x00é", ID: "task\x00id", Partition: 7}
	for _, kind := range []byte{barrierKind, metadataKind, stateKind} {
		raw := encodeKey(kind, spec, "store\x00", "key\x00é")
		got, err := decodeKey(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.application != spec.ApplicationID || got.task != spec.ID || got.partition != spec.Partition || got.kind != kind {
			t.Fatalf("scope %+v", got)
		}
		if kind == stateKind && (got.store != "store\x00" || got.key != "key\x00é") {
			t.Fatal("binary fields changed")
		}
		if kind == barrierKind && got.key != "key\x00é" {
			t.Fatal("barrier token changed")
		}
	}
	raw := encodeKey(stateKind, spec, "store", "key")
	for n := 0; n < len(raw); n++ {
		if _, err := decodeKey(raw[:n]); err == nil {
			t.Fatalf("accepted truncated key at %d", n)
		}
	}
	if _, err := decodeKey(append(raw, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	other := spec
	other.Partition++
	if transactionID(spec) == transactionID(other) || bytes.Equal(encodeKey(metadataKind, spec, "", ""), encodeKey(metadataKind, other, "", "")) {
		t.Fatal("partition ownership collided")
	}
}
func FuzzChangelogKey(f *testing.F) {
	f.Add("app", "task", "store", "key", int32(0))
	f.Add("a\x00", "é", "", "\x00", int32(7))
	f.Fuzz(func(t *testing.T, app, task, store, key string, partition int32) {
		if partition < 0 {
			return
		}
		spec := execution.Spec{ApplicationID: app, ID: task, Partition: partition}
		decoded, err := decodeKey(encodeKey(stateKind, spec, store, key))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.application != app || decoded.task != task || decoded.partition != partition || decoded.store != store || decoded.key != key {
			t.Fatal("changelog key round-trip mismatch")
		}
	})
}
