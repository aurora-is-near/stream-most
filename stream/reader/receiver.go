package reader

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Receiver interface {
	// Returning false would stop consumption
	HandleMsg(ctx context.Context, msg messages.NatsMessage) bool

	// Might be called concurrently
	// Returning false would stop consumption
	HandleNewKnownSeq(seq uint64) bool

	// It's guaranteed to be the very last callback which is only called once
	HandleFinish(err error)
}

type CbReceiver struct {
	HandleMsgCb         func(ctx context.Context, msg messages.NatsMessage) bool
	HandleNewKnownSeqCb func(seq uint64) bool
	HandleFinishCb      func(err error)
}

func NewCbReceiver() *CbReceiver {
	return &CbReceiver{
		HandleMsgCb:         func(ctx context.Context, msg messages.NatsMessage) bool { return true },
		HandleNewKnownSeqCb: func(seq uint64) bool { return true },
		HandleFinishCb:      func(err error) {},
	}
}

func (cr *CbReceiver) WithHandleMsgCb(cb func(ctx context.Context, msg messages.NatsMessage) bool) *CbReceiver {
	cr.HandleMsgCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleNewKnownSeqCb(cb func(seq uint64) bool) *CbReceiver {
	cr.HandleNewKnownSeqCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleFinishCb(cb func(err error)) *CbReceiver {
	cr.HandleFinishCb = cb
	return cr
}

func (cr *CbReceiver) HandleMsg(ctx context.Context, msg messages.NatsMessage) bool {
	return cr.HandleMsgCb(ctx, msg)
}

func (cr *CbReceiver) HandleNewKnownSeq(seq uint64) bool {
	return cr.HandleNewKnownSeqCb(seq)
}

func (cr *CbReceiver) HandleFinish(err error) {
	cr.HandleFinishCb(err)
}
