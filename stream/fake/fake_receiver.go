package fake

import (
	"context"
	"fmt"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

// Static assertion
var _ reader.Receiver = (*FakeReceiver)(nil)

type FakeReceiver struct {
	ch chan messages.NatsMessage
}

func (r *FakeReceiver) Ch() <-chan messages.NatsMessage {
	return r.ch
}

func (r *FakeReceiver) HandleMsg(ctx context.Context, msg messages.NatsMessage) {
	select {
	case <-ctx.Done():
		return
	case r.ch <- msg:
	}
}

func (r *FakeReceiver) HandleFinish(err error) {
	close(r.ch)
}

func (r *FakeReceiver) GetAll(limit int, timeout time.Duration) ([]messages.NatsMessage, error) {
	result := []messages.NatsMessage{}
	deadlineCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("fake reader stuck")
		case el, ok := <-r.ch:
			if !ok {
				return result, nil
			}
			result = append(result, el)
			if len(result) > limit {
				return nil, fmt.Errorf("limit exceeded")
			}
		}
	}
}

func NewFakeReceiver(capacity int) *FakeReceiver {
	return &FakeReceiver{
		ch: make(chan messages.NatsMessage),
	}
}
