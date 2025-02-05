package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type ReadOnly interface {
	Ctx() context.Context
	DoneCtx() context.Context

	StopInitiated() bool
	StoppingReason() error
	InterruptedExternally() bool
	Done() bool
	WaitDone(ctx context.Context) (done bool)
}

type External interface {
	ReadOnly

	ReadOnly() ReadOnly
	SendInterrupt(interruptReason error) (isPrimaryReason bool)
	InterruptAndWait(ctx context.Context, interruptReason error) (isPrimaryReason bool, done bool)
}

type Lifecycle struct {
	ctx       context.Context
	cancelCtx context.CancelFunc

	doneCtx       context.Context
	cancelDoneCtx context.CancelFunc

	customInterruptionErr        error
	parentContextInterruptionErr error
	stoppingReason               error
	setStoppingReasonOnce        sync.Once
}

func NewLifecycle(ctx context.Context, customInterruptionErr error) *Lifecycle {
	l := &Lifecycle{
		customInterruptionErr:        customInterruptionErr,
		parentContextInterruptionErr: fmt.Errorf("%w: interrupted via parent context", customInterruptionErr),
	}
	l.ctx, l.cancelCtx = context.WithCancel(ctx)
	l.doneCtx, l.cancelDoneCtx = context.WithCancel(context.Background())
	return l
}

func (l *Lifecycle) External() External {
	return l
}

func (l *Lifecycle) ReadOnly() ReadOnly {
	return l
}

func (l *Lifecycle) initiateStop(reason error) (isPrimaryReason bool) {
	isPrimaryReason = false
	l.setStoppingReasonOnce.Do(func() {
		select {
		case <-l.ctx.Done():
			l.stoppingReason = l.parentContextInterruptionErr
		default:
			l.stoppingReason = reason
			l.cancelCtx()
			isPrimaryReason = true
		}
	})
	return isPrimaryReason
}

func (l *Lifecycle) Ctx() context.Context {
	return l.ctx
}

func (l *Lifecycle) DoneCtx() context.Context {
	return l.doneCtx
}

func (l *Lifecycle) StopInitiated() bool {
	select {
	case <-l.ctx.Done():
		return true
	default:
		return false
	}
}

func (l *Lifecycle) StoppingReason() error {
	if !l.StopInitiated() {
		return nil
	}
	l.initiateStop(l.parentContextInterruptionErr) // fallback if it was stopped via parent context
	return l.stoppingReason
}

func (l *Lifecycle) InterruptedExternally() bool {
	return errors.Is(l.stoppingReason, l.customInterruptionErr)
}

func (l *Lifecycle) Done() bool {
	select {
	case <-l.doneCtx.Done():
		return true
	default:
		return false
	}
}

func (l *Lifecycle) WaitDone(ctx context.Context) (done bool) {
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

func (l *Lifecycle) SendStop(reason error, markAsInterruption bool) (isPrimaryReason bool) {
	if markAsInterruption {
		reason = fmt.Errorf("%w: reason: %w", l.customInterruptionErr, reason)
	}
	return l.initiateStop(reason)
}

func (l *Lifecycle) StopAndWait(ctx context.Context, reason error, markAsInterruption bool) (isPrimaryReason bool, done bool) {
	isPrimaryReason = l.SendStop(reason, markAsInterruption)
	return isPrimaryReason, l.WaitDone(ctx)
}

func (l *Lifecycle) SendInterrupt(interruptReason error) (isPrimaryReason bool) {
	return l.SendStop(interruptReason, true)
}

func (l *Lifecycle) InterruptAndWait(ctx context.Context, interruptReason error) (isPrimaryReason bool, done bool) {
	return l.StopAndWait(ctx, interruptReason, true)
}

func (l *Lifecycle) MarkDone() {
	l.initiateStop(nil) // fallback if it wasn't stopped before
	l.cancelDoneCtx()
}
