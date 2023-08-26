package tipwatcher

import (
	"context"
	"errors"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrCantDecode = errors.New("can't decode")

type TipInfo struct {
	streamInfo *jetstream.StreamInfo
	lastMsg    *messages.BlockMessage
	err        error
	outdated   chan struct{}
}

func (t *TipInfo) StreamInfo() *jetstream.StreamInfo {
	return t.streamInfo
}

func (t *TipInfo) LastMsg() *messages.BlockMessage {
	return t.lastMsg
}

func (t *TipInfo) Error() error {
	return t.err
}

func (t *TipInfo) Outdated() <-chan struct{} {
	return t.outdated
}

func FetchTip(ctx context.Context, input stream.Interface) *TipInfo {
	res := &TipInfo{
		outdated: make(chan struct{}),
	}

	if res.streamInfo, res.err = input.GetInfo(ctx); res.err != nil {
		res.err = fmt.Errorf("unable to get stream info for stream '%s': %w", input.Name(), res.err)
		return res
	}

	if state := res.streamInfo.State; state.Msgs == 0 || state.LastSeq == 0 || state.FirstSeq > state.LastSeq {
		return res
	}

	msg, err := input.Get(ctx, res.streamInfo.State.LastSeq)
	if err != nil {
		res.err = fmt.Errorf(
			"unable to fetch last msg (#%d) from stream '%s': %w",
			res.streamInfo.State.LastSeq,
			input.Name(),
			err,
		)
		return res
	}

	blockMsg, err := formats.Active().ParseMsg(msg)
	if err != nil {
		res.err = fmt.Errorf(
			"unable to decode last message (#%d) of stream '%s': %s (%w)",
			res.streamInfo.State.LastSeq,
			input.Name(),
			err.Error(),
			ErrCantDecode,
		)
		return res
	}

	res.lastMsg = blockMsg

	return res
}
