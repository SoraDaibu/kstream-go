// benchmark measures the in-process core; it does not qualify broker performance.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/SoraDaibu/kstream-go/stream"
)

const seed uint64 = 14695981039346656037
const prime uint64 = 1099511628211

type checksumBackend struct {
	sum     uint64
	numeric bool
}

func (b *checksumBackend) Open(context.Context, execution.Spec) (uint64, execution.Snapshot, error) {
	return 1, execution.Snapshot{State: make(state.Image), StreamTime: -1}, nil
}
func (b *checksumBackend) byte(v byte) { b.sum = (b.sum ^ uint64(v)) * prime }
func (b *checksumBackend) text(s string) {
	for i := 0; i < len(s); i++ {
		b.byte(s[i])
	}
	b.byte(0)
}
func (b *checksumBackend) Commit(_ context.Context, _ execution.Spec, _ uint64, tx execution.Transaction) error {
	for _, r := range tx.Outputs {
		key, err := serde.String.Decode(r.Key)
		if err != nil {
			return err
		}
		b.text(key)
		if b.numeric {
			v, err := serde.Int64.Decode(r.Value)
			if err != nil {
				return err
			}
			for shift := 56; shift >= 0; shift -= 8 {
				b.byte(byte(uint64(v) >> shift))
			}
		} else {
			v, err := serde.String.Decode(r.Value)
			if err != nil {
				return err
			}
			b.text(v)
		}
	}
	return nil
}
func rss() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && f[0] == "VmHWM:" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n
		}
	}
	return -1
}
func run() error {
	if len(os.Args) != 4 {
		return fmt.Errorf("usage: benchmark filter|count|reduce|aggregate records warmup")
	}
	scenario := os.Args[1]
	n, err := strconv.Atoi(os.Args[2])
	if err != nil || n < 1 {
		return fmt.Errorf("invalid record count")
	}
	warmup, err := strconv.Atoi(os.Args[3])
	if err != nil || warmup < 0 {
		return fmt.Errorf("invalid warmup")
	}
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	switch scenario {
	case "filter":
		s.Filter("nonempty", func(r record.Record[string, string]) bool { return !r.NullValue && r.Value != "" }).To("out")
	case "count":
		stream.Count(s, "counts").ToStream().To("out")
	case "reduce":
		values := stream.MapValues(s, "length", serde.Int64, func(v string, _ bool) (int64, bool) { return int64(len(v)), false })
		values.GroupByKey().Reduce("sums", func(a, v int64) (int64, bool) { return a + v, false }).ToStream().To("out")
	case "aggregate":
		stream.Aggregate(s.GroupByKey(), "sums", serde.Int64, func() (int64, bool) { return 0, false },
			func(_ string, v string, a int64, _ bool) (int64, bool) { return a + int64(len(v)), false }).ToStream().To("out")
	default:
		return fmt.Errorf("unknown scenario")
	}
	plan, err := b.Build()
	if err != nil {
		return err
	}
	backend := &checksumBackend{sum: seed, numeric: scenario != "filter"}
	ctx := context.Background()
	task, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "benchmark", ID: "0"}, backend)
	if err != nil {
		return err
	}
	defer task.Close()
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	value := []byte("value")
	process := func(start, end int) error {
		for i := start; i < end; i++ {
			if err := task.Process(ctx, record.Bytes{Topic: "in", Offset: int64(i), Timestamp: int64(i), Key: keys[i%64], Value: value}); err != nil {
				return err
			}
		}
		return nil
	}
	if err = process(0, warmup); err != nil {
		return err
	}
	backend.sum = seed
	start := time.Now()
	if err = process(warmup, warmup+n); err != nil {
		return err
	}
	elapsed := time.Since(start).Nanoseconds()
	fmt.Printf("go,%s,%d,%d,%016x,%d\n", scenario, n, elapsed, backend.sum, rss())
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
