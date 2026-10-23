package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func main() {
	ctx := context.Background()
	b := stream.NewStreamsBuilder()
	words := stream.Stream(b, "words", serde.String, serde.String)
	stream.Count(words.Filter("nonempty", func(r record.Record[string, string]) bool { return r.Value != "" }), "counts").ToStream().To("counts-out")
	topology, err := b.Build()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(topology.Describe())
	backend := memory.New()
	spec := execution.Spec{ApplicationID: "wordcount", ID: "0", Partition: 0}
	task, err := topology.NewTask(ctx, spec, backend)
	if err != nil {
		log.Fatal(err)
	}
	for offset, word := range []string{"go", "kafka", "go"} {
		if err := task.Process(ctx, record.Bytes{Topic: "words", Offset: int64(offset), Timestamp: int64(offset), Key: []byte(word), Value: []byte(word)}); err != nil {
			log.Fatal(err)
		}
	}
	for _, r := range backend.Outputs(spec) {
		count, err := serde.Int64.Decode(r.Value)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %d\n", r.Key, count)
	}
}
