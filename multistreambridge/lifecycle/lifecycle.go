package lifecycle

import "context"

type Lifecycle struct {
	stopCtx       context.Context
	cancelStopCtx context.CancelFunc

	doneCtx       context.Context
	cancelDoneCtx context.CancelFunc
}

func NewLifecycle(ctx context.Context) *Lifecycle {
	l := &Lifecycle{}
	l.stopCtx, l.cancelStopCtx = context.WithCancel(ctx)
	l.doneCtx, l.cancelDoneCtx = context.WithCancel(context.Background())
	return l
}

// Pass `util.FinishedContext()` context to not wait for stop
func (l *Lifecycle) Stop(ctx context.Context) bool {
	l.cancelStopCtx()

	select {
	case <-l.doneCtx.Done():
		return true
	default:
	}

	select {
	case <-l.doneCtx.Done():
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Lifecycle) StopCtx() context.Context {
	return l.stopCtx
}

func (l *Lifecycle) StopSent() bool {
	select {
	case <-l.stopCtx.Done():
		return true
	default:
		return false
	}
}

func (l *Lifecycle) DoneCtx() context.Context {
	return l.doneCtx
}

func (l *Lifecycle) IsDone() bool {
	select {
	case <-l.doneCtx.Done():
		return true
	default:
		return false
	}
}

func (l *Lifecycle) MarkDone() {
	l.cancelDoneCtx()
}
