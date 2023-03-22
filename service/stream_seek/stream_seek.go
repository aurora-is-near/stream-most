package stream_seek

import (
	"errors"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/domain/new_format"
	"github.com/aurora-is-near/stream-most/stream"
)

var (
	ErrNotFound = errors.New("not found")
)

// StreamSeek provides methods to seek for something on the given stream
type StreamSeek struct {
	stream stream.Interface
}

// SeekAnnouncementWithHeightBelow returns the sequence number of the latest block announcement which height
// is below a given one. notBefore and notAfter are given in sequence numbers and are used to limit the search range.
func (p *StreamSeek) SeekAnnouncementWithHeightBelow(height uint64, notBefore uint64, notAfter uint64) (uint64, error) {
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return 0, err
	}

	if info.State.LastSeq == 0 {
		return 0, ErrNotFound
	}

	if notBefore > info.State.LastSeq {
		return 0, ErrNotFound
	}

	for seq := notAfter; seq >= notBefore; seq-- {
		d, err := p.stream.Get(seq)
		if err != nil {
			return 0, err
		}

		message, err := new_format.ProtoToMessage(d.Data)
		if err != nil {
			return 0, err
		}

		switch msg := message.(type) {
		case messages.BlockAnnouncement:
			if msg.Block.Height < height {
				return msg.Block.Height, nil
			}
		}
	}

	return 0, ErrNotFound
}

func NewStreamSeek(streamInterface stream.Interface) *StreamSeek {
	return &StreamSeek{
		stream: streamInterface,
	}
}
