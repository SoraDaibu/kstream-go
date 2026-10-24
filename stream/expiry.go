package stream

// windowExpiryHeap is task-owned derived state. It is discarded on transaction
// failure along with its poisoned task and rebuilt from committed stores.
type windowExpiry struct {
	end int64
	key string
}
type windowExpiryHeap []windowExpiry

func (h *windowExpiryHeap) add(entry windowExpiry) {
	*h = append(*h, entry)
	i := len(*h) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if (*h)[parent].end <= entry.end {
			break
		}
		(*h)[i] = (*h)[parent]
		i = parent
	}
	(*h)[i] = entry
}
func (h windowExpiryHeap) down(i int) {
	entry := h[i]
	for {
		child := i*2 + 1
		if child >= len(h) || child < 0 {
			break
		}
		if right := child + 1; right < len(h) && h[right].end < h[child].end {
			child = right
		}
		if entry.end <= h[child].end {
			break
		}
		h[i] = h[child]
		i = child
	}
	h[i] = entry
}
func (h windowExpiryHeap) initialize() {
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}
func (h *windowExpiryHeap) pop() windowExpiry {
	entry := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	(*h)[last] = windowExpiry{} // Release the removed key's backing bytes.
	*h = (*h)[:last]
	if last > 0 {
		h.down(0)
	}
	return entry
}
