package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SoraDaibu/kstream-go/record"
)

type Strategy uint8

const (
	BoundedWorkers Strategy = iota
	GoroutinePerTask
)

type Lane struct {
	Task    *Task
	Records []record.Bytes
}

// Run processes finite independent task lanes. Each task may appear only once.
// A task's entire lane runs serially. Workers bounds active tasks in bounded mode.
func Run(ctx context.Context, lanes []Lane, workers int, strategy Strategy) error {
	return RunBatches(ctx, lanes, workers, strategy, 1)
}

// RunBatches preserves per-task record order and commits at most batchSize
// records at a time. The caller bounds transaction memory through batchSize.
func RunBatches(ctx context.Context, lanes []Lane, workers int, strategy Strategy, batchSize int) error {
	if batchSize < 1 {
		return errors.New("batch size must be positive")
	}
	if workers < 1 {
		return errors.New("workers must be positive")
	}
	if strategy != BoundedWorkers && strategy != GoroutinePerTask {
		return errors.New("unknown scheduling strategy")
	}
	seen := make(map[*Task]bool)
	for _, l := range lanes {
		if l.Task == nil || seen[l.Task] {
			return errors.New("nil or duplicate task lane")
		}
		seen[l.Task] = true
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	run := func(l Lane) {
		for start := 0; start < len(l.Records); {
			end := start + batchSize
			if end < start || end > len(l.Records) {
				end = len(l.Records)
			}
			if err := ctx.Err(); err != nil {
				return
			}
			if err := l.Task.ProcessBatch(ctx, l.Records[start:end]); err != nil {
				once.Do(func() { first = fmt.Errorf("task %s: %w", l.Task.spec.ID, err); cancel() })
				return
			}
			start = end
		}
	}
	if strategy == GoroutinePerTask {
		for _, l := range lanes {
			wg.Add(1)
			go func(l Lane) { defer wg.Done(); run(l) }(l)
		}
	} else {
		jobs := make(chan Lane)
		n := workers
		if n > len(lanes) {
			n = len(lanes)
		}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for l := range jobs {
					run(l)
				}
			}()
		}
	send:
		for _, l := range lanes {
			select {
			case <-ctx.Done():
				break send
			case jobs <- l:
			}
		}
		close(jobs)
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
