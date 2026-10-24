package main

import (
	"testing"

	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestVerifierHandlesPartitionInterleavingAndRejectsDuplicates(t *testing.T) {
	for _, scenario := range []string{"filter", "count"} {
		v := &verifier{scenario: scenario, seen: make([]int64, 2)}
		// Per-key order is intact even when independent broker partitions arrive
		// in a different global order. Count values recover exact input IDs.
		rows := []struct {
			key, filter string
			count, want int64
		}{{"1", "1", 1, 1}, {"1", "3", 2, 3}, {"0", "0", 1, 0}, {"0", "2", 2, 2}}
		var duplicate *kgo.Record
		for _, test := range rows {
			value := []byte(test.filter)
			if scenario == "count" {
				var err error
				value, err = serde.Int64.Encode(test.count)
				if err != nil {
					t.Fatal(err)
				}
			}
			row := &kgo.Record{Key: []byte(test.key), Value: value}
			got, err := v.inputID(row)
			if err != nil || got != test.want {
				t.Fatal(scenario, got, err)
			}
			duplicate = row
		}
		if _, err := v.inputID(duplicate); err == nil {
			t.Fatal("duplicate output passed correctness gate", scenario)
		}
	}
}
func TestVerifierRejectsMissingAndInvalidOutput(t *testing.T) {
	for _, row := range []*kgo.Record{{Key: nil, Value: []byte("1")}, {Key: []byte("2"), Value: []byte("1")}, {Key: []byte("0"), Value: nil}, {Key: []byte("0"), Value: []byte("2")}} {
		v := &verifier{scenario: "filter", seen: make([]int64, 2)}
		if _, err := v.inputID(row); err == nil {
			t.Fatal("invalid output passed correctness gate", row)
		}
	}
	value, err := serde.Int64.Encode(2)
	if err != nil {
		t.Fatal(err)
	}
	v := &verifier{scenario: "count", seen: make([]int64, 2)}
	if _, err := v.inputID(&kgo.Record{Key: []byte("0"), Value: value}); err == nil {
		t.Fatal("missing count update passed gate")
	}
}
