package stream_peek

import (
	"context"
	"errors"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
	errors2 "github.com/pkg/errors"
)

var (
	ErrCorruptedTip = errors.New("corrupted tip")
	ErrEmptyStream  = errors.New("empty output stream")
)

// StreamPeek provides methods to peek onto the tip of the given stream
type StreamPeek struct {
	stream stream.Interface
}

func (p *StreamPeek) GetTip() (messages.BlockMessage, error) {
	info, err := p.stream.GetInfo(context.Background())
	if err != nil {
		return nil, err
	}

	if info.State.Msgs == 0 || info.State.LastSeq == 0 || info.State.FirstSeq > info.State.LastSeq {
		return nil, nil
	}

	msg, err := p.stream.Get(context.Background(), info.State.LastSeq)
	if err != nil {
		return nil, err
	}

	return formats.Active().ParseMsg(msg)
}

// GetTipHeight returns the last announced block's height.
// Unlike the old-format stream-bridge's peeking, this one doesn't return height
// of a last fully written block, but the height of the last block announcement.
// Hence, it can still miss some shard-blocks yet.
func (p *StreamPeek) GetTipHeight() (uint64, error) {
	info, err := p.stream.GetInfo(context.Background())
	if err != nil {
		return 0, err
	}

	if info.State.Msgs == 0 || info.State.LastSeq == 0 || info.State.FirstSeq > info.State.LastSeq {
		return 0, ErrEmptyStream
	}

	msg, err := p.stream.Get(context.Background(), info.State.LastSeq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, ErrEmptyStream
		}
		return 0, err
	}

	// We need to inspect the message to see its height
	message, err := formats.Active().ParseMsg(msg)
	if err != nil {
		return 0, errors2.Wrap(ErrCorruptedTip, err.Error())
	}

	return message.GetHeight(), nil
}

func NewStreamPeek(streamInterface stream.Interface) *StreamPeek {
	return &StreamPeek{
		stream: streamInterface,
	}
}
