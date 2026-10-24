package kafka

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/state"
)

func TestRestoreConfigurationBounds(t *testing.T) {
	for _, config := range []Config{
		{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RestoreBatchSize: -1},
		{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RestoreBatchSize: 65537},
		{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RestoreFetchBytes: -1},
		{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RestoreFetchBytes: 1023},
	} {
		if _, err := New(config); err == nil {
			t.Fatal("unbounded/invalid restore configuration accepted")
		}
	}
	b, err := New(Config{Brokers: []string{"unused:9092"}, ChangelogTopic: "state"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.config.RestoreBatchSize != 256 || b.config.RestoreFetchBytes != 8<<20 {
		t.Fatal("restore defaults changed")
	}
}

func TestBrokerBoundedStreamRestoreMatchesLegacyAndPropagatesSinkFailure(t *testing.T) {
	cfg := brokerConfig(t)
	cfg.RestoreBatchSize = 7
	cfg.RestoreFetchBytes = 1024
	ctx := brokerContext(t)
	spec := execution.Spec{ApplicationID: unique("streamed"), ID: "0"}
	first := backendFor(t, cfg)
	g, _, err := first.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]state.Mutation, 0, 100)
	expected := make(state.Image)
	for i := 0; i < 100; i++ {
		row := state.Mutation{Store: "s", Key: fmt.Sprint(i), Value: []byte(fmt.Sprintf("value-%d", i))}
		rows = append(rows, row)
		expected.Apply(row)
	}
	if err = first.Commit(ctx, spec, g, execution.Transaction{Mutations: rows, NextOffsets: map[string]int64{"left": 5, "right": 9}, StreamTime: 123, ScopeTimes: map[string]int64{"upstream": 120, "downstream": 70}}); err != nil {
		t.Fatal(err)
	}
	rows = []state.Mutation{{Store: "s", Key: "0", Value: nil}, {Store: "s", Key: "1", Value: []byte{}}, {Store: "s", Key: "2", Value: []byte("updated")}}
	for _, row := range rows {
		expected.Apply(row)
	}
	if err = first.Commit(ctx, spec, g, execution.Transaction{Mutations: rows, NextOffsets: map[string]int64{"left": 7}, StreamTime: 124}); err != nil {
		t.Fatal(err)
	}
	first.Close()
	restored := backendFor(t, cfg)
	failure := errors.New("restore sink failed")
	batches := 0
	if _, _, err = restored.OpenInto(ctx, spec, func(rows []state.Mutation) error {
		batches++
		if len(rows) > 7 {
			t.Fatal("restore batch exceeded configured bound")
		}
		if batches == 2 {
			return failure
		}
		return nil
	}); !errors.Is(err, failure) {
		t.Fatal("sink failure was hidden", err)
	}
	if err = restored.Commit(ctx, spec, 1, execution.Transaction{}); !errors.Is(err, execution.ErrPoisoned) {
		t.Fatal("partial restore remained usable", err)
	}
	image := make(state.Image)
	batches = 0
	_, checkpoint, err := restored.OpenInto(ctx, spec, func(rows []state.Mutation) error {
		batches++
		if len(rows) > 7 {
			t.Fatal("restore batch exceeded configured bound")
		}
		for _, row := range rows {
			image.Apply(row)
		}
		return nil
	})
	if !reflect.DeepEqual(checkpoint.ScopeTimes, map[string]int64{"upstream": 120, "downstream": 70}) {
		t.Fatal("stream restore dropped logical clocks", checkpoint)
	}
	if err != nil || batches < 2 || !reflect.DeepEqual(image, expected) || checkpoint.StreamTime != 124 || !reflect.DeepEqual(checkpoint.NextOffsets, map[string]int64{"left": 7, "right": 9}) {
		t.Fatal("streamed restore lost state/checkpoint", batches, checkpoint, err)
	}
	_, legacy, err := restored.Open(ctx, spec)
	if err != nil || !reflect.DeepEqual(image, legacy.State) || !reflect.DeepEqual(checkpoint.NextOffsets, legacy.NextOffsets) || checkpoint.StreamTime != legacy.StreamTime || !reflect.DeepEqual(checkpoint.ScopeTimes, legacy.ScopeTimes) {
		t.Fatal("legacy and streamed restore differ", err)
	}
}
