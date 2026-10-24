package execution

// ScopedContext shares task state and input metadata while exposing the clock
// of one connected processing stage. Create during setup under task ownership.
// Like Task, it must only be used synchronously by the current task owner.
type ScopedContext struct {
	*Task
	scope string
}

func (t *Task) ScopedContext(scope string) *ScopedContext {
	if scope == "" {
		panic("empty processing clock scope")
	}
	return &ScopedContext{Task: t, scope: scope}
}

func (c *ScopedContext) StreamTime() int64 {
	clocks := c.Task.scopeTimes
	if c.Task.pending != nil {
		clocks = c.Task.pending.ScopeTimes
	}
	if stamp, ok := clocks[c.scope]; ok {
		return stamp
	}
	return -1
}

// Observe advances this stage before decoding or filtering an accepted source
// record. Staged clocks are copied once per transaction and publish atomically
// with outputs, state and input offsets. Replay never reaches this callback.
func (c *ScopedContext) Observe(stamp int64) {
	t := c.Task
	t.requirePending()
	if stamp < 0 || stamp > t.pending.StreamTime {
		panic("processing clock is outside the accepted input prefix")
	}
	if stamp <= c.StreamTime() {
		return
	}
	if !t.pendingScopesCopy {
		t.pending.ScopeTimes = copyScopeTimes(t.pending.ScopeTimes)
		if t.pending.ScopeTimes == nil {
			t.pending.ScopeTimes = make(map[string]int64)
		}
		t.pendingScopesCopy = true
	}
	t.pending.ScopeTimes[c.scope] = stamp
}

func copyScopeTimes(clocks map[string]int64) map[string]int64 {
	if clocks == nil {
		return nil
	}
	return cloneOffsets(clocks)
}
