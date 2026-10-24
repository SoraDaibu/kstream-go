package stream

import "testing"

func TestReduceExpiryTracksOneEntryPerUnexpiredKey(t *testing.T) {
	s := timeWindowState{tracked: make(map[string]struct{})}
	for i := 0; i < 100000; i++ {
		// Deleting/recreating the same open aggregate must not retain one
		// derived heap entry per update until the event-time clock advances.
		s.track("same-window", 10)
	}
	s.track("other-window", 20)
	if len(s.expiry) != 2 || len(s.tracked) != 2 {
		t.Fatal("derived expiry memory grew with updates", len(s.expiry), len(s.tracked))
	}
	if first, second := s.expiry.pop(), s.expiry.pop(); first.end != 10 || second.end != 20 {
		t.Fatal("deduplicated expiry changed deadline ordering", first, second)
	}
}
