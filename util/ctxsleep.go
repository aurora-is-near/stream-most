package util

import (
	"context"
	"time"
)

func CtxSleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		if !t.Stop() {
			<-t.C
		}
		return false
	case <-t.C:
		return true
	}
}
