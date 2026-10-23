// kafka-count demonstrates explicitly assigned Kafka input and transactionally
// checkpointed counts. Provision topics and external partition ownership first.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"

	kafkadapter "github.com/SoraDaibu/kstream-go/adapters/kafka"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func run(ctx context.Context) error {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	changelog := flag.String("changelog", "kstream-state", "pre-provisioned compacted changelog")
	inputTopic := flag.String("input", "words", "input topic")
	outputTopic := flag.String("output", "counts", "output topic")
	app := flag.String("app", "kafka-count", "stable application ID")
	id := flag.String("task", "0", "stable task ID")
	partition := flag.Int("partition", 0, "externally assigned source partition")
	batch := flag.Int("batch", 256, "maximum records per atomic transaction")
	flag.Parse()
	if *partition < 0 || int64(*partition) > math.MaxInt32 || *batch < 1 {
		return fmt.Errorf("invalid partition or batch size")
	}
	builder := stream.NewStreamsBuilder()
	stream.Count(stream.Stream(builder, *inputTopic, serde.String, serde.String), "counts").ToStream().To(*outputTopic)
	plan, err := builder.Build()
	if err != nil {
		return err
	}
	backend, err := kafkadapter.New(kafkadapter.Config{Brokers: strings.Split(*brokers, ","), ChangelogTopic: *changelog})
	if err != nil {
		return err
	}
	defer backend.Close()
	task, err := plan.NewTask(ctx, execution.Spec{ApplicationID: *app, ID: *id, Partition: int32(*partition)}, backend)
	if err != nil {
		return err
	}
	input, err := backend.Input(*inputTopic)
	if err != nil {
		return err
	}
	defer input.Close()
	for {
		rows, err := input.Next(ctx, *batch)
		if err != nil {
			return err
		}
		if err = task.ProcessBatch(ctx, rows); err != nil {
			return fmt.Errorf("task failed; restart to restore committed Kafka state: %w", err)
		}
	}
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
