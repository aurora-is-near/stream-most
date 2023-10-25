package reader

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Receiver interface {
	// Returning non-nil error would stop reader, final error would be given error wrapped with ErrInterrupted
	HandleMsg(ctx context.Context, msg messages.NatsMessage) error

	// Might be called concurrently
	// Returning non-nil error would stop reader, final error would be given error wrapped with ErrInterrupted
	HandleNewKnownSeq(ctx context.Context, seq uint64) error

	// It's guaranteed to be the very last callback which is only called once
	// Provides final error
	// If reader is stopped by Stop() method or by receiver callback, final error is always a subclass of ErrInterrupted
	HandleFinish(err error)
}

// Static assertion
var _ Receiver = (*CbReceiver)(nil)

type CbReceiver struct {
	HandleMsgCb         func(ctx context.Context, msg messages.NatsMessage) error
	HandleNewKnownSeqCb func(ctx context.Context, seq uint64) error
	HandleFinishCb      func(err error)
}

func NewCbReceiver() *CbReceiver {
	return &CbReceiver{
		HandleMsgCb:         func(ctx context.Context, msg messages.NatsMessage) error { return nil },
		HandleNewKnownSeqCb: func(ctx context.Context, seq uint64) error { return nil },
		HandleFinishCb:      func(err error) {},
	}
}

func (cr *CbReceiver) WithHandleMsgCb(cb func(ctx context.Context, msg messages.NatsMessage) error) *CbReceiver {
	cr.HandleMsgCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleNewKnownSeqCb(cb func(ctx context.Context, seq uint64) error) *CbReceiver {
	cr.HandleNewKnownSeqCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleFinishCb(cb func(err error)) *CbReceiver {
	cr.HandleFinishCb = cb
	return cr
}

func (cr *CbReceiver) HandleMsg(ctx context.Context, msg messages.NatsMessage) error {
	return cr.HandleMsgCb(ctx, msg)
}

func (cr *CbReceiver) HandleNewKnownSeq(ctx context.Context, seq uint64) error {
	return cr.HandleNewKnownSeqCb(ctx, seq)
}

func (cr *CbReceiver) HandleFinish(err error) {
	cr.HandleFinishCb(err)
}
