package workpool

import (
	"context"
	"runtime"
	"sync"
)

type Workpool struct {
	ctx    context.Context
	cancel func()
	jobs   chan func(context.Context)
	wg     sync.WaitGroup
}

func New(workers int, queueSize uint) *Workpool {
	maxprocs := runtime.GOMAXPROCS(0)
	if workers < 1 || workers > maxprocs {
		workers = maxprocs
	}

	w := &Workpool{
		jobs: make(chan func(context.Context), queueSize),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())

	w.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go w.runWorker()
	}

	return w
}

func NewAuto() *Workpool {
	return New(0, 8192)
}

func (w *Workpool) Add(ctx context.Context, job func(context.Context)) bool {
	select {
	case <-ctx.Done():
		return false
	case w.jobs <- job:
		return true
	}
}

func (w *Workpool) StopImmediately(waitStop bool) {
	w.cancel()
	if waitStop {
		w.wg.Wait()
	}
}

func (w *Workpool) StopWhenFinished(waitStop bool) {
	close(w.jobs)
	if waitStop {
		w.wg.Wait()
	}
}

func (w *Workpool) WaitStop() {
	w.wg.Wait()
}

func (w *Workpool) runWorker() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		select {
		case <-w.ctx.Done():
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}
			job(w.ctx)
		}
	}
}
