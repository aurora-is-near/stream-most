package streamstate

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
)

// Static assertion
var _ blockio.State = (*State)(nil)

type State struct {
	Info    *jetstream.StreamInfo
	LastMsg blockio.Msg
	Err     error
}

func (s *State) FirstSeq() uint64 {
	return s.Info.State.FirstSeq
}

func (s *State) LastSeq() uint64 {
	return s.Info.State.LastSeq
}

func (s *State) Tip() blockio.Msg {
	return s.LastMsg
}

func Fetch(ctx context.Context, input stream.Interface) *State {
	res := &State{}

	if res.Info, res.Err = input.GetInfo(ctx); res.Err != nil {
		res.Err = fmt.Errorf("unable to get stream info for stream '%s': %w", input.Name(), res.Err)
		return res
	}

	if state := res.Info.State; state.Msgs == 0 || state.LastSeq == 0 || state.FirstSeq > state.LastSeq {
		return res
	}

	tipSeq := res.Info.State.LastSeq
	msg, err := input.Get(ctx, tipSeq)
	if err != nil {
		// Checking if message fell out of stream
		if res.Info, res.Err = input.GetInfo(ctx); res.Err != nil {
			res.Err = fmt.Errorf("unable to get stream info for stream '%s': %w", input.Name(), res.Err)
			return res
		}
		if tipSeq < res.Info.State.FirstSeq {
			// Confirmed, Message fell out of stream
			return res
		}

		res.Err = fmt.Errorf(
			"unable to fetch last msg (#%d) from stream '%s': %w",
			res.Info.State.LastSeq,
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
