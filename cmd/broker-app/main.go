// broker-app runs the Go side of the paired broker experiment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/SoraDaibu/kstream-go/adapters/kafka"
	"github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	pebbledb "github.com/cockroachdb/pebble"
	"github.com/twmb/franz-go/pkg/kgo"
)

func run() (err error) {
	scenario := flag.String("scenario", "count", "filter or count")
	brokers := flag.String("brokers", "localhost:9092", "broker addresses")
	app := flag.String("application", "", "application/group ID")
	input := flag.String("input", "", "source topic")
	output := flag.String("output", "", "output topic")
	changelog := flag.String("changelog", "", "changelog topic")
	directory := flag.String("state", "", "state directory")
	warmup := flag.Int64("warmup", 100000, "warmup record boundary for observation")
	cpuProfile := flag.String("cpuprofile", "", "diagnostic whole-process CPU profile path")
	heapProfile := flag.String("heapprofile", "", "diagnostic post-cleanup cumulative allocation profile path")
	outputCapacity := flag.Int("output-capacity", 0, "initial transaction output capacity; zero keeps append growth")
	flag.Parse()
	if *outputCapacity < 0 || *outputCapacity > 65536 {
		return errors.New("output capacity must be between 0 and 65536")
	}
	finishProfiles, err := startProfiles(*cpuProfile, *heapProfile)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, finishProfiles()) }()
	engine, err := pebble.Open(*directory, pebble.Config{Options: &pebbledb.Options{DisableWAL: true}})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	builder := stream.NewStreamsBuilder()
	source := stream.Stream(builder, *input, serde.String, serde.String)
	switch *scenario {
	case "filter":
		source.Filter("nonempty", func(r record.Record[string, string]) bool { return !r.NullValue && r.Value != "" }).To(*output)
	case "count":
		source.GroupByKey().Count("counts").ToStream().To(*output)
	default:
		return errors.New("unknown scenario")
	}
	plan, err := builder.Build()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config := kafka.GroupConfig{Kafka: kafka.Config{Brokers: strings.Split(*brokers, ","), ChangelogTopic: *changelog, Options: []kgo.Opt{kgo.ProducerBatchCompression(kgo.NoCompression())}}, ApplicationID: *app, InstanceID: "benchmark-worker", Topics: []string{*input}, BatchSize: 4096, Options: []kgo.Opt{kgo.RebalanceTimeout(5 * time.Minute)}}
	observation := batchObservation{Event: "batch_metrics", warmupLimit: *warmup}
	config.OnBatch = observation.add
	err = kafka.RunGroup(ctx, config, func(ctx context.Context, spec execution.Spec, backend execution.Backend) (*execution.Task, error) {
		return plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task), execution.WithOutputCapacity(*outputCapacity))
	})
	if encodeErr := json.NewEncoder(os.Stdout).Encode(observation); encodeErr != nil {
		err = errors.Join(err, encodeErr)
	}
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
