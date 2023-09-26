package streaminput

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

var _ reader.Receiver = (*receiver)(nil)

type receiver struct {
	errCh chan error
	s     *session
}

func newReceiver(s *session) *receiver {
	return &receiver{
		errCh: make(chan error, 1),
		s:     s,
	}
}

func (r *receiver) HandleMsg(ctx context.Context, msg messages.NatsMessage) {
	m, ok := blockdecode.ScheduleBlockDecoding(ctx, msg)
	if !ok {
		return
	}

	select {
	case <-ctx.Done():
		return
	case r.s.ch <- m:
	}

	r.s.nextSeq = msg.GetSequence() + 1
}

func (r *receiver) HandleFinish(err error) {
	r.errCh <- err
}

func (r *receiver) HandleNewKnownSeq(seq uint64) {}
