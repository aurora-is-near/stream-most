package tolerance

type Tolerance struct {
	counter int
	max     int
}

func NewTolerance(max int) *Tolerance {
	return &Tolerance{
		counter: 0,
		max:     max,
	}
}

func (t *Tolerance) Tolerate(cnt int) bool {
	t.counter += cnt
	if t.max >= 0 && t.counter > t.max {
		return false
	}
	return true
}

func (t *Tolerance) Reset() {
	t.counter = 0
}
