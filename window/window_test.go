package window_test

import (
	"github.com/SoraDaibu/kstream-go/window"
	"math"
	"testing"
)

func TestBoundsAndGrace(t *testing.T) {
	w := window.Tumbling{Size: 10, Grace: 5}
	for _, tt := range []struct{ timestamp, start, end int64 }{{0, 0, 10}, {9, 0, 10}, {10, 10, 20}} {
		s, e, err := w.Bounds(tt.timestamp)
		if err != nil || s != tt.start || e != tt.end {
			t.Fatalf("%+v: %d %d %v", tt, s, e, err)
		}
	}
	if w.Closed(10, 14) || !w.Closed(10, 15) {
		t.Fatal("incorrect grace boundary")
	}
	if _, _, err := w.Bounds(-1); err == nil {
		t.Fatal("negative timestamp accepted")
	}
	if _, _, err := w.Bounds(math.MaxInt64); err == nil {
		t.Fatal("overflow accepted")
	}
	if w.Closed(math.MaxInt64, math.MaxInt64) {
		t.Fatal("grace overflow closed window")
	}
}

func TestHoppingStartsAgainstMembershipModel(t *testing.T) {
	for size := int64(1); size <= 24; size++ {
		for advance := int64(1); advance <= size; advance++ {
			w := window.Hopping{Size: size, Advance: advance, Grace: 5}
			for ts := int64(0); ts < 80; ts++ {
				first, last, err := w.Starts(ts)
				if err != nil {
					t.Fatal(err)
				}
				var expected []int64
				for start := int64(0); start <= ts; start += advance {
					if ts < start+size {
						expected = append(expected, start)
					}
				}
				if len(expected) == 0 || first != expected[0] || last != expected[len(expected)-1] || (last-first)/advance+1 != int64(len(expected)) {
					t.Fatalf("size=%d advance=%d ts=%d: %d..%d expected %v", size, advance, ts, first, last, expected)
				}
			}
		}
	}
}

func TestHoppingInvalidAndOverflowBounds(t *testing.T) {
	for _, w := range []window.Hopping{{}, {Size: 10}, {Size: 10, Advance: 11}, {Size: 10, Advance: -1}, {Size: 10, Advance: 1, Grace: -1}} {
		if _, _, err := w.Starts(0); err == nil {
			t.Fatal("invalid hopping window accepted", w)
		}
	}
	w := window.Hopping{Size: 10, Advance: 3, Grace: 5}
	for _, ts := range []int64{-1, math.MaxInt64} {
		if _, _, err := w.Starts(ts); err == nil {
			t.Fatal("invalid timestamp accepted", ts)
		}
	}
	if w.Closed(10, 14) || !w.Closed(10, 15) || w.Closed(math.MaxInt64, math.MaxInt64) {
		t.Fatal("hopping grace boundary overflow")
	}
	// Valid arithmetic must also terminate near the maximum representable time.
	w = window.Hopping{Size: 2, Advance: 1}
	first, last, err := w.Starts(math.MaxInt64 - 2)
	if err != nil || first != math.MaxInt64-3 || last != math.MaxInt64-2 {
		t.Fatal(first, last, err)
	}
}
