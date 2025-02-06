package jobrestarter

import (
	"context"
	"fmt"
	"time"

	"github.com/aurora-is-near/stream-most/multistreambridge/lifecycle"
)

type Job interface {
	Run(ctx context.Context)
}

type JobRestarter[T Job] struct {
	job                  T
	minIterationDuration time.Duration
	lifecycle            *lifecycle.Lifecycle
}

func StartJobRestarter[T Job](ctx context.Context, job T, minIterationDuration time.Duration) *JobRestarter[T] {
	j := &JobRestarter[T]{
		job:                  job,
		minIterationDuration: minIterationDuration,
		lifecycle:            lifecycle.NewLifecycle(ctx, fmt.Errorf("job-restarter interrupted")),
	}
	go j.run()
	return j
}

func (j *JobRestarter[T]) Job() T {
	return j.job
}

func (j *JobRestarter[T]) Lifecycle() lifecycle.External {
	return j.lifecycle
}

func (j *JobRestarter[T]) run() {
	defer j.lifecycle.MarkDone()

	iterationTicker := time.NewTicker(j.minIterationDuration)
	defer iterationTicker.Stop()

	for !j.lifecycle.StopInitiated() {
		j.job.Run(j.lifecycle.Ctx())

		select {
		case <-j.lifecycle.Ctx().Done():
			return
		case <-iterationTicker.C:
		}
	}
}
