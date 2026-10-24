// trace executes the same finite input trace as the independent Java oracle.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	persistent "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

func nullable(v string) []byte {
	if v == "~" {
		return nil
	}
	b := make([]byte, len(v))
	copy(b, v)
	return b
}
func field(v []byte) string {
	if v == nil {
		return "~"
	}
	return string(v)
}
func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: trace scenario input.tsv")
	}
	scenario := os.Args[1]
	b := stream.NewStreamsBuilder()
	var table stream.KTable[string, string]
	if scenario == "join" || scenario == "left-join" || scenario == "join-null" || scenario == "table" {
		table = stream.Table(b, "table", "lookup", serde.String, serde.String)
	}
	if scenario == "table" {
		table.ToStream().To("out")
	} else {
		s := stream.Stream(b, "in", serde.String, serde.String)
		switch scenario {
		case "filter":
			s.Filter("nonempty", func(r record.Record[string, string]) bool { return !r.NullValue && r.Value != "" }).To("out")
		case "filter-not":
			s.FilterNot("not-nonempty", func(r record.Record[string, string]) bool { return !r.NullValue && r.Value != "" }).To("out")
		case "mapvalues":
			stream.MapValues(s, "values", serde.String, func(v string, null bool) (string, bool) {
				if null {
					return "null", false
				}
				return v + "!", false
			}).To("out")
		case "flatmapvalues":
			stream.FlatMapValues(s, "values", serde.String, func(v string, null bool, yield func(string, bool) error) error {
				if err := yield(v, null); err != nil {
					return err
				}
				if null {
					return yield("", true)
				}
				return yield(v+"2", false)
			}).To("out")
		case "flatmap":
			stream.FlatMap(s, "twice", serde.String, serde.String, func(r record.Record[string, string], next processor.Forward[string, string]) error {
				if err := next(r); err != nil {
					return err
				}
				if !r.NullKey {
					r.Key += "2"
				}
				return next(r)
			}).To("out")
		case "merge":
			right := stream.Stream(b, "right", serde.String, serde.String)
			stream.Merge(s, right, "merged").To("out")
		case "count":
			stream.Map(stream.Count(s, "counts").ToStream(), "format", serde.String, serde.String, func(r record.Record[string, int64]) record.Record[string, string] {
				return record.Record[string, string]{Key: r.Key, Value: strconv.FormatInt(r.Value, 10), Timestamp: r.Timestamp}
			}).To("out")
		case "reduce":
			s.GroupByKey().Reduce("reduced", func(a, v string) (string, bool) {
				return a + ":" + v, v == "!delete"
			}).ToStream().To("out")
		case "aggregate", "aggregate-null":
			aggregated := stream.Aggregate(s.GroupByKey(), "aggregated", serde.Int64,
				func() (int64, bool) { return 10, scenario == "aggregate-null" },
				func(k, v string, old int64, oldNull bool) (int64, bool) {
					if oldNull {
						old = 1000
					}
					return old + int64(len(k)+len(v)), v == "!delete"
				})
			stream.MapValues(aggregated.ToStream(), "format", serde.String, func(v int64, null bool) (string, bool) {
				return strconv.FormatInt(v, 10), null
			}).To("out")
		case "join":
			stream.Join(s, table, "join", serde.String, func(a, c string) string { return a + ":" + c }).To("out")
		case "join-null":
			stream.JoinNullable(s, table, "join", serde.String, func(a, c string) (string, bool) { return a + ":" + c, a == "!null-output" }).To("out")
		case "left-join":
			stream.LeftJoin(s, table, "left", serde.String, func(a, c string, rightNull bool) (string, bool) {
				if rightNull {
					c = "missing"
				}
				return a + ":" + c, a == "!null-output"
			}).To("out")
		case "window":
			stream.Map(stream.WindowedCount(s, "windows", window.Tumbling{Size: 10, Grace: 5}), "format-window", serde.String, serde.String, func(r record.Record[window.Key[string], int64]) record.Record[string, string] {
				return record.Record[string, string]{Key: fmt.Sprintf("%s@%d:%d", r.Key.Key, r.Key.Start, r.Key.End), Value: strconv.FormatInt(r.Value, 10), Timestamp: r.Timestamp}
			}).To("out")
		default:
			return fmt.Errorf("unknown scenario %q", scenario)
		}
	}
	p, err := b.Build()
	if err != nil {
		return err
	}
	backend := memory.New()
	id := execution.Spec{ApplicationID: "oracle", ID: "0"}
	ctx := context.Background()
	var transactionGroup *execution.Group
	var taskBackend execution.Backend = backend
	if os.Getenv("KSTREAM_TRANSACTION_GROUP") == "1" {
		transactionGroup, err = execution.NewGroup(backend)
		if err != nil {
			return err
		}
		taskBackend = transactionGroup
	}
	var options []execution.Option
	if directory := os.Getenv("KSTREAM_STATE_DIR"); directory != "" {
		engine, err := persistent.Open(directory, persistent.Config{SyncWrites: true})
		if err != nil {
			return err
		}
		defer engine.Close()
		options = append(options, execution.WithStoreFactory(engine.Task))
	}
	task, err := p.NewTask(ctx, id, taskBackend, options...)
	if err != nil {
		return err
	}
	defer task.Close()
	file, err := os.Open(os.Args[2])
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	batchSize := 1
	if raw := os.Getenv("KSTREAM_BATCH_SIZE"); raw != "" {
		batchSize, err = strconv.Atoi(raw)
		if err != nil || batchSize < 1 {
			return fmt.Errorf("KSTREAM_BATCH_SIZE must be positive")
		}
	}
	batch := make([]record.Bytes, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		var err error
		if transactionGroup == nil {
			err = task.ProcessBatch(ctx, batch)
		} else {
			err = transactionGroup.Process(ctx, []execution.TaskBatch{{Task: task, Records: batch}})
		}
		batch = batch[:0]
		return err
	}
	offsets := make(map[string]int64)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			return fmt.Errorf("trace requires four fields")
		}
		ts, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return err
		}
		batch = append(batch, record.Bytes{Topic: f[0], Offset: offsets[f[0]], Timestamp: ts, Key: nullable(f[2]), Value: nullable(f[3])})
		offsets[f[0]]++
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	for _, r := range backend.Outputs(id) {
		fmt.Printf("%s\t%s\t%s\t%d\n", r.Topic, field(r.Key), field(r.Value), r.Timestamp)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
