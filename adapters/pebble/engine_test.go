package pebble_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	adapter "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/state"
	pebbledb "github.com/cockroachdb/pebble"
)

func TestPersistentNamespacesBatchesOwnedValuesAndReopen(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	engine, err := adapter.Open(directory, adapter.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	left, err := engine.Scope("left\x00")
	if err != nil {
		t.Fatal(err)
	}
	right, err := engine.Scope("right")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.Scope("left\x00"); !errors.Is(err, adapter.ErrInUse) {
		t.Fatal("duplicate logical owner accepted", err)
	}
	if err = engine.Close(); !errors.Is(err, adapter.ErrInUse) {
		t.Fatal("engine closed with active owners", err)
	}
	value := []byte("one")
	if err = left.ApplyBatch([]state.Mutation{{Store: "a", Key: "x", Value: value}, {Store: "a", Key: "empty", Value: []byte{}}, {Store: "a\x00", Key: "x", Value: []byte("separate")}}); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if err = right.ApplyBatch([]state.Mutation{{Store: "a", Key: "x", Value: []byte("right")}}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := left.Get("a", "x")
	if err != nil || !ok || string(got) != "one" {
		t.Fatal("namespace or write-buffer isolation failed")
	}
	got[0] = 'Y'
	got, _, _ = left.Get("a", "x")
	if string(got) != "one" {
		t.Fatal("read buffer was borrowed")
	}
	actual := make(map[string]string)
	if err = left.Scan("a", func(key string, value []byte) {
		if key == "empty" && value == nil {
			t.Error("scan lost empty/non-null distinction")
		}
		actual[key] = string(value)
		if len(value) > 0 {
			value[0] = 'Z'
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, map[string]string{"x": "one", "empty": ""}) {
		t.Fatalf("store prefix scan leaked: %v", actual)
	}
	got, _, _ = left.Get("a", "x")
	if string(got) != "one" {
		t.Fatal("scan buffer was borrowed")
	}
	empty, ok, err := left.Get("a", "empty")
	if err != nil || !ok || empty == nil || len(empty) != 0 {
		t.Fatal("empty became a tombstone")
	}
	if err = left.ApplyBatch([]state.Mutation{{Store: "a", Key: "x", Value: nil}, {Store: "a", Key: "new", Value: []byte("two")}}); err != nil {
		t.Fatal(err)
	}
	if err = left.Close(); err != nil {
		t.Fatal(err)
	}
	if err = right.Close(); err != nil {
		t.Fatal(err)
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = adapter.Open(directory, adapter.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	left, err = engine.Scope("left\x00")
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err = engine.Scope("right")
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if _, ok, err = left.Get("a", "x"); err != nil || ok {
		t.Fatal("reopen lost delete")
	}
	got, ok, err = left.Get("a", "new")
	if err != nil || !ok || string(got) != "two" {
		t.Fatal("reopen lost state", err)
	}
	if err = left.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = left.Get("a\x00", "x"); err != nil || ok {
		t.Fatal("reset left a logical store behind")
	}
	got, ok, err = right.Get("a", "x")
	if err != nil || !ok || string(got) != "right" {
		t.Fatal("reset crossed task namespace")
	}
}
func TestSynchronousModeRejectsDisabledWAL(t *testing.T) {
	if _, err := adapter.Open(filepath.Join(t.TempDir(), "state"), adapter.Config{SyncWrites: true, Options: &pebbledb.Options{DisableWAL: true}}); err == nil {
		t.Fatal("sync durability accepted without WAL")
	}
}
