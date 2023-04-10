package stream_peek

import (
	"errors"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
)

var (
	ErrCorruptedTip = errors.New("corrupted tip")
)

// StreamPeek provides methods to peek onto the tip of the given stream
type StreamPeek struct {
	stream stream.Interface
}

func (p *StreamPeek) GetTip() (messages.AbstractNatsMessage, error) {
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return nil, err
	}

	if info.State.LastSeq == 0 {
		return nil, nil
	}

	msg, err := p.stream.Get(info.State.LastSeq)
	if err != nil {
		return nil, err
	}

	return formats.Active().ParseRawMsg(msg)
}

// GetTipHeight returns the last announced block's height.
// Unlike the old-format stream-bridge's peeking, this one doesn't return height
// of a last fully written block, but the height of the last block announcement.
// Hence, it can still miss some shard-blocks yet.
func (p *StreamPeek) GetTipHeight() (uint64, error) {
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return 0, err
	}

	if info.State.LastSeq == 0 {
		return 0, nil
	}

	msg, err := p.stream.Get(info.State.LastSeq)
	if err != nil {
		return 0, err
	}

	// We need to inspect the message to see its height
	message, err := formats.Active().ParseRawMsg(msg)
	if err != nil {
		return 0, err
	}

	return message.GetBlock().Height, nil
}

func NewStreamPeek(streamInterface stream.Interface) *StreamPeek {
	return &StreamPeek{
		stream: streamInterface,
	}
}
