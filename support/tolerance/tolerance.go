package tolerance

import "sync/atomic"

type Tolerance struct {
	max     int64
	counter atomic.Int64
}

func NewTolerance(max int64) *Tolerance {
	return &Tolerance{
		max: max,
	}
}

func (t *Tolerance) Tolerate(add int64) bool {
	for {
		cnt := t.counter.Load()
		if t.max >= 0 && cnt+add > t.max {
			return false
		}
		if t.counter.CompareAndSwap(cnt, cnt+add) {
			return true
		}
	}
}

func (t *Tolerance) GetCounter() int64 {
	return t.counter.Load()
}

func (t *Tolerance) Reset() {
	t.counter.Store(0)
}
