package v3

import (
	"sync"
)

type cachedString struct {
	value    string
	saveOnce sync.Once
}

func (c *cachedString) get(b []byte) string {
	c.saveOnce.Do(func() {
		c.value = string(b)
	})
	return c.value
}
