package state_test

import (
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/state"
)

func TestMemoryStoreOwnedValuesTombstonesAndReset(t *testing.T) {
	store := state.NewMemory()
	value := []byte("value")
	if err := store.ApplyBatch([]state.Mutation{{Store: "a", Key: "key", Value: value}, {Store: "a", Key: "empty", Value: []byte{}}, {Store: "b", Key: "key", Value: []byte("other")}}); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, ok, err := store.Get("a", "key")
	if err != nil || !ok || string(got) != "value" {
		t.Fatal("borrowed write buffer")
	}
	got[0] = 'Y'
	again, _, _ := store.Get("a", "key")
	if string(again) != "value" {
		t.Fatal("borrowed read buffer")
	}
	empty, ok, err := store.Get("a", "empty")
	if err != nil || !ok || empty == nil || len(empty) != 0 {
		t.Fatal("empty value became null")
	}
	if err = store.ApplyBatch([]state.Mutation{{Store: "a", Key: "key", Value: nil}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = store.Get("a", "key"); err != nil || ok {
		t.Fatal("delete failed")
	}
	if err = store.Scan("b", func(_ string, v []byte) { v[0] = 'Z' }); err != nil {
		t.Fatal(err)
	}
	got, _, _ = store.Get("b", "key")
	if string(got) != "other" {
		t.Fatal("scan leaked owned bytes")
	}
	if err = store.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ = store.Get("b", "key"); ok {
		t.Fatal("reset failed")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Get("a", "key"); !errors.Is(err, state.ErrClosed) {
		t.Fatal(err)
	}
}
