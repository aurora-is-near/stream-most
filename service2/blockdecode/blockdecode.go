package blockdecode

import (
	"context"
	"sync"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/aurora-is-near/stream-most/support/workpool"
)

const (
	queueSize = 8192
)

var (
	startOnce sync.Once
	decoders  *workpool.Workpool
)

func EnsureDecodersRunning() {
	startOnce.Do(func() {
		decoders = workpool.New(0, queueSize)
	})
}

func StopDecoders() {
	decoders.StopImmediately(true)
}

func ScheduleBlockDecoding(ctx context.Context, msg messages.NatsMessage) (blockio.Msg, bool) {
	m := &Msg{
		original:     msg,
		decodingDone: make(chan struct{}),
	}
	if decoders.Add(ctx, m.decode) {
		return m, true
	}
	return nil, false
}
