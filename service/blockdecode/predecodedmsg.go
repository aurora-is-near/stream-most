package blockdecode

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
)

// Static assertion
var _ blockio.Msg = (*PredecodedMsg)(nil)

type PredecodedMsg struct {
	Predecoded *messages.BlockMessage
}

func (m *PredecodedMsg) Get() messages.NatsMessage {
	return m.Predecoded.Msg
}

func (m *PredecodedMsg) GetDecoded(ctx context.Context) (*messages.BlockMessage, error) {
	return m.Predecoded, nil
}

func NewPredecodedMsg(predecoded *messages.BlockMessage) *PredecodedMsg {
	return &PredecodedMsg{Predecoded: predecoded}
}
