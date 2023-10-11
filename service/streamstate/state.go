package streamstate

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
)

type State struct {
	Info       *jetstream.StreamInfo
	LastMsgSeq uint64
	LastMsg    blockio.Msg
	Err        error
}

func Fetch(ctx context.Context, input *stream.Stream) *State {
	res := &State{}

	if res.Info, res.Err = input.GetInfo(ctx); res.Err != nil {
		res.Err = fmt.Errorf("unable to get stream info for stream '%s': %w", input.Name(), res.Err)
		return res
	}

	res.LastMsgSeq = res.Info.State.LastSeq

	if state := res.Info.State; state.Msgs == 0 || state.LastSeq == 0 || state.FirstSeq > state.LastSeq {
		return res
	}

	msg, err := input.Get(ctx, res.LastMsgSeq)
	if err != nil {
		// Checking if message fell out of stream
		if res.Info, res.Err = input.GetInfo(ctx); res.Err != nil {
			res.Err = fmt.Errorf("unable to get stream info for stream '%s': %w", input.Name(), res.Err)
			return res
		}
		if res.LastMsgSeq < res.Info.State.FirstSeq {
			// Confirmed, Message fell out of stream
			return res
		}

		res.Err = fmt.Errorf(
			"unable to fetch last msg (#%d) from stream '%s': %w",
			res.LastMsgSeq,
			input.Name(),
			err,
		)
		return res
	}

	blockdecode.EnsureDecodersRunning()
	var ok bool
	if res.LastMsg, ok = blockdecode.ScheduleBlockDecoding(ctx, msg); !ok {
		res.Err = fmt.Errorf("unable to schedule block decoding: canceled")
		return res
	}

	return res
}
