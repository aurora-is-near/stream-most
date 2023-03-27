package stream_peek

import (
	"errors"
	"github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
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

	// We need to inspect the message to see its height
	message, err := v3.ProtoToMessage(msg.Data)
	if err != nil {
		return nil, err
	}

	switch m := message.(type) {
	case *messages.BlockAnnouncement:
		return messages.NatsMessage{
			Msg: &nats.Msg{
				Subject: msg.Subject,
				Header:  msg.Header,
				Data:    msg.Data,
			},
			Metadata: &nats.MsgMetadata{
				Sequence: nats.SequencePair{
					Stream: msg.Sequence,
				},
			},
			Announcement: m,
		}, nil
	case *messages.BlockShard:
		return messages.NatsMessage{
			Msg: &nats.Msg{
				Subject: msg.Subject,
				Header:  msg.Header,
				Data:    msg.Data,
			},
			Metadata: &nats.MsgMetadata{
				Sequence: nats.SequencePair{
					Stream: msg.Sequence,
				},
			},
			Shard: m,
		}, nil
	}

	return nil, ErrCorruptedTip
}

// GetTipHeight returns the last announced block'u height.
// Unlike the old-format stream-bridge'u peeking, this one doesn't return height
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
	message, err := v3.ProtoToMessage(msg.Data)
	if err != nil {
		return 0, err
	}

	switch m := message.(type) {
	case messages.BlockAnnouncement:
		return m.Block.Height, nil
	case messages.BlockShard:
		return m.Block.Height, nil
	}
	return 0, ErrCorruptedTip
}

func NewStreamPeek(streamInterface stream.Interface) *StreamPeek {
	return &StreamPeek{
		stream: streamInterface,
	}
}
