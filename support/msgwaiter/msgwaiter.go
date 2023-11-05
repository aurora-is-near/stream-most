package msgwaiter

import (
	"context"
	"sync/atomic"

	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/util"
)

type MsgWaiter struct {
	nextMsg    atomic.Pointer[blockio.Msg]
	hasNextMsg chan struct{}
	cb         func(blockio.Msg)
	job        *util.Job
}

func Start(ctx context.Context, cb func(blockio.Msg)) *MsgWaiter {
	w := &MsgWaiter{
		hasNextMsg: make(chan struct{}, 1),
		cb:         cb,
	}
	w.job = util.StartJobLoop(ctx, w.runIteration)
	return w
}

func (w *MsgWaiter) PutNextMsg(msg blockio.Msg) {
	w.nextMsg.Store(&msg)
	select {
	case w.hasNextMsg <- struct{}{}:
	default:
	}
}

func (w *MsgWaiter) Stop(ctx context.Context) bool {
	return w.job.Stop(ctx)
}

func (w *MsgWaiter) runIteration(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-w.hasNextMsg:
	}

	bPtr := w.nextMsg.Load()
	if bPtr == nil || *bPtr == nil {
		return nil
	}

	b := *bPtr
	b.GetDecoded(ctx)

	select {
	case <-ctx.Done():
	default:
		w.cb(b)
	}

	return nil
}
