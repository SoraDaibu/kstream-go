package stream

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/window"
)

// timeWindowState belongs to one operator in one task incarnation. Its heap/set
// are derived indexes; only the clock and live aggregate rows are durable.
type timeWindowState struct {
	task        *execution.Task
	store       string
	w           window.Hopping
	expiry      windowExpiryHeap
	initialized bool
	clock       int64
	// Reduce can delete/recreate open rows. Counts leave this nil because they
	// only delete on expiry and never need to deduplicate a live window key.
	tracked map[string]struct{}
}

func (s *timeWindowState) track(key string, end int64) {
	if s.tracked != nil {
		if _, exists := s.tracked[key]; exists {
			return
		}
		s.tracked[key] = struct{}{}
	}
	s.expiry.add(windowExpiry{end: end, key: key})
}

// advance is called only for records accepted by an operator's null policy.
// Window eligibility uses this operator's transactional clock, not another
// branch's task clock. It returns arithmetic bounds without a window collection.
func (s *timeWindowState) advance(timestamp int64) (int64, int64, int64, error) {
	first, last, err := s.w.Starts(timestamp)
	if err != nil {
		return 0, 0, 0, err
	}
	if !s.initialized {
		// The clock has this operator as its sole owner. Load its committed
		// value once, then stage every advance and discard the cached value
		// with a failed task, just like the derived expiry index.
		s.clock = -1
		raw, exists, err := s.task.StoreGet(s.store, "")
		if err != nil {
			return 0, 0, 0, err
		}
		if exists {
			s.clock, err = serde.Int64.Decode(raw)
			if err != nil {
				return 0, 0, 0, err
			}
		}
		var invalid error
		if err := s.task.StoreScan(s.store, func(key string, _ []byte) {
			if key == "" {
				return // Operator-local clock metadata.
			}
			if len(key) < 8 {
				invalid = errors.New("invalid stored window key")
				return
			}
			start := int64(binary.BigEndian.Uint64([]byte(key[:8])))
			if start < 0 || start > math.MaxInt64-s.w.Size {
				invalid = errors.New("invalid stored window bounds")
				return
			}
			s.expiry = append(s.expiry, windowExpiry{end: start + s.w.Size, key: key})
			if s.tracked != nil {
				s.tracked[key] = struct{}{}
			}
		}); err != nil {
			return 0, 0, 0, err
		}
		if invalid != nil {
			return 0, 0, 0, invalid
		}
		s.expiry.initialize()
		s.initialized = true
	}
	if timestamp > s.clock {
		s.clock = timestamp
		encoded, err := serde.Int64.Encode(s.clock)
		if err != nil {
			return 0, 0, 0, err
		}
		s.task.StorePut(s.store, "", encoded)
		for len(s.expiry) > 0 && s.w.Closed(s.expiry[0].end, s.clock) {
			entry := s.expiry.pop()
			s.task.StorePut(s.store, entry.key, nil)
			delete(s.tracked, entry.key)
		}
	}
	return first, last, s.clock, nil
}
