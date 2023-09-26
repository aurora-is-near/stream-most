package streamoutput

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/streamstate"
)

// Static assertions
var (
	_ blockio.State = stateWithTip{}
	_ blockio.Msg   = stateWithTip{}
)

type stateWithTip struct {
	state *streamstate.State
	tip   *messages.BlockMessage
}

func (s stateWithTip) FirstSeq() uint64 {
	return s.state.FirstSeq()
}

func (s stateWithTip) LastSeq() uint64 {
	return s.tip.Msg.GetSequence()
}

func (s stateWithTip) Tip() blockio.Msg {
	return s
}

func (s stateWithTip) Get() messages.NatsMessage {
	return s.tip.Msg
}

func (s stateWithTip) GetDecoded(ctx context.Context) (*messages.BlockMessage, error) {
	return s.tip, nil
}
