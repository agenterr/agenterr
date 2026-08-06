package alerts

import "sync/atomic"

// counter is a tiny wrapper around atomic.Int64 kept as its own type so
// call sites read as intent (add/load) rather than raw atomic ops.
type counter struct {
	v atomic.Int64
}

func (c *counter) add(n int64) { c.v.Add(n) }
func (c *counter) load() int64 { return c.v.Load() }
