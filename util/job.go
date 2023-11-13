package util

import (
	"context"
	"time"
)

type Job struct {
	ctx     context.Context
	cancel  func()
	err     error
	stopped chan struct{}
}

func StartJob(ctx context.Context, fn func(ctx context.Context) error) *Job {
	j := &Job{
		stopped: make(chan struct{}),
	}
	j.ctx, j.cancel = context.WithCancel(ctx)

	go func() {
		defer close(j.stopped)
		j.err = fn(j.ctx)
	}()

	return j
}

func StartJobLoop(ctx context.Context, fn func(ctx context.Context) error) *Job {
	return StartJob(ctx, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := fn(ctx); err != nil {
					return err
				}
			}
		}
	})
}

func StartJobLoopWithInterval(ctx context.Context, interval time.Duration, firstImmediateRun bool, fn func(ctx context.Context) error) *Job {
	return StartJob(ctx, func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		if firstImmediateRun {
			if err := fn(ctx); err != nil {
				return err
			}
		}

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					if err := fn(ctx); err != nil {
						return err
					}
				}
			}
		}
	})
}

func (j *Job) Stop(ctx context.Context) bool {
	j.cancel()
	select {
	case <-j.stopped:
		return true
	case <-ctx.Done():
		return false
	}
}

func (j *Job) Stopped() <-chan struct{} {
	return j.stopped
}

func (j *Job) Error() error {
	select {
	case <-j.stopped:
		return j.err
	default:
		return nil
	}
}
