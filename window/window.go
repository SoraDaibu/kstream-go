// Package window defines event-time window boundaries in Unix milliseconds.
package window

import (
	"errors"
	"math"
)

type Tumbling struct{ Size, Grace int64 }

// Hopping windows have size Size and start every Advance milliseconds. Windows
// overlap when Advance < Size. All durations are integral Unix milliseconds.
type Hopping struct{ Size, Advance, Grace int64 }

func (w Hopping) Validate() error {
	if w.Size <= 0 || w.Advance <= 0 || w.Advance > w.Size || w.Grace < 0 {
		return errors.New("window requires 0 < advance <= size and nonnegative grace")
	}
	return nil
}

// Starts returns the inclusive first and last window start containing timestamp.
// Enumerate them in steps of Advance. No collection is allocated.
func (w Hopping) Starts(timestamp int64) (int64, int64, error) {
	if err := w.Validate(); err != nil {
		return 0, 0, err
	}
	if timestamp < 0 {
		return 0, 0, errors.New("negative timestamp")
	}
	last := timestamp - timestamp%w.Advance
	if last > math.MaxInt64-w.Size {
		return 0, 0, errors.New("window end overflow")
	}
	first := int64(0)
	if timestamp >= w.Size {
		first = ((timestamp-w.Size)/w.Advance + 1) * w.Advance
	}
	return first, last, nil
}

func (w Hopping) Closed(end, streamTime int64) bool {
	return (Tumbling{Size: w.Size, Grace: w.Grace}).Closed(end, streamTime)
}

func (w Tumbling) Validate() error {
	if w.Size <= 0 || w.Grace < 0 {
		return errors.New("positive window size and nonnegative grace required")
	}
	return nil
}
func (w Tumbling) Bounds(timestamp int64) (int64, int64, error) {
	if err := w.Validate(); err != nil {
		return 0, 0, err
	}
	if timestamp < 0 {
		return 0, 0, errors.New("negative timestamp")
	}
	start := timestamp - timestamp%w.Size
	if start > math.MaxInt64-w.Size {
		return 0, 0, errors.New("window end overflow")
	}
	return start, start + w.Size, nil
}
func (w Tumbling) Closed(end, streamTime int64) bool {
	if end > math.MaxInt64-w.Grace {
		return false
	}
	return streamTime >= end+w.Grace
}

type Key[K any] struct {
	Key        K
	Start, End int64
}
