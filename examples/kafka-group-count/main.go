// kafka-group-count runs the experimental group-managed transaction worker.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/SoraDaibu/kstream-go/adapters/kafka"
	"github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func setting(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func run() (err error) {
	brokers, instance := os.Getenv("KAFKA_BROKERS"), os.Getenv("KSTREAM_INSTANCE_ID")
	if brokers == "" || instance == "" {
		return fmt.Errorf("set KAFKA_BROKERS and a stable unique KSTREAM_INSTANCE_ID")
	}
	application := setting("KSTREAM_APPLICATION_ID", "kstream-group-count")
	input, output := setting("INPUT_TOPIC", "words"), setting("OUTPUT_TOPIC", "counts-out")
	engine, err := pebble.Open(setting("STATE_DIR", "./state"), pebble.Config{SyncWrites: true})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	builder := stream.NewStreamsBuilder()
	stream.Stream(builder, input, serde.String, serde.String).GroupByKey().Count("counts").ToStream().To(output)
	plan, err := builder.Build()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config := kafka.GroupConfig{Kafka: kafka.Config{Brokers: strings.Split(brokers, ","), ChangelogTopic: setting("CHANGELOG_TOPIC", application+"-state")}, ApplicationID: application, InstanceID: instance, Topics: []string{input}, BatchSize: 256}
	err = kafka.RunGroup(ctx, config, func(ctx context.Context, spec execution.Spec, backend execution.Backend) (*execution.Task, error) {
		return plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task))
	})
	if err == context.Canceled {
		return nil
	}
	return err
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
