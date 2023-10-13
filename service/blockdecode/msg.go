package blockdecode

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
)

// Static assertion
var _ blockio.Msg = (*Msg)(nil)

type Msg struct {
	original      messages.NatsMessage
	decoded       *messages.BlockMessage
	decodingError error
	decodingDone  chan struct{}
}

func (m *Msg) Get() messages.NatsMessage {
	return m.original
}

func (m *Msg) GetDecoded(ctx context.Context) (*messages.BlockMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.decodingDone:
		return m.decoded, m.decodingError
	}
}

func (m *Msg) decode(ctx context.Context) {
	m.decoded, m.decodingError = formats.Active().ParseMsg(m.original)
	close(m.decodingDone)
}
