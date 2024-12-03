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
			// Needed for compatibility with both pre-1.23 and post-1.23 go versions
			select {
			case <-t.C:
			default:
			}
		}
		return false
	case <-t.C:
		return true
	}
}
