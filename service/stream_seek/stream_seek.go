package stream_seek

import (
	"github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

var (
	ErrNotFound = errors.New("not found")
)

// StreamSeek provides methods to seek for something on the given stream
type StreamSeek struct {
	stream stream.Interface
}

// SeekShards TODO: fetch multiple messages at once instead
func (p *StreamSeek) SeekShards(from, to uint64, forBlock *string) ([]messages.AbstractNatsMessage, error) {
	var shards []messages.AbstractNatsMessage

	if from == 0 {
		from = 1
	}

	for seq := to; seq >= from; seq-- {
		d, err := p.stream.Get(seq)
		if err != nil {
			continue
		}

		message, err := v3.ProtoToMessage(d.Data)
		if err != nil {
			return shards, err
		}

		switch msg := message.(type) {
		case *messages.BlockShard:
			if forBlock == nil || (msg.Block.Hash == *forBlock) {
				shards = append(shards, messages.NatsMessage{
					Msg: &nats.Msg{
						Subject: d.Subject,
						Header:  d.Header,
						Data:    d.Data,
					},
					Metadata: &nats.MsgMetadata{
						Sequence: nats.SequencePair{
							Stream: d.Sequence,
						},
					},
					Shard: msg,
				})
			}
		}
	}

	return shards, nil
}

// SeekAnnouncementWithHeightBelow returns the sequence number of the latest block announcement which height
// is below a given one. notBefore and notAfter are given in sequence numbers and are used to limit the search range.
// Set notAfter to 0 to search until the end of the stream.
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

	// To not cross the stream'u boundaries
	if notAfter > info.State.LastSeq {
		notAfter = info.State.LastSeq
	}

	// if notAfter is not specified, we go until the end of the stream
	if notAfter == 0 {
		notAfter = info.State.LastSeq
	}

	for seq := notAfter; seq >= notBefore; seq-- {
		d, err := p.stream.Get(seq)
		if err != nil {
			return 0, err
		}

		message, err := v3.ProtoToMessage(d.Data)
		if err != nil {
			return 0, err
		}

		switch msg := message.(type) {
		case *messages.BlockAnnouncement:
			if msg.Block.Height < height {
				return seq, nil
			}
		}
	}

	return 0, ErrNotFound
}

// SeekFirstAnnouncementBetween returns the sequence number of the first block announcement which sequence number
// is between from and to. Both from and to are given in sequence numbers and are used to limit the search range.
func (p *StreamSeek) SeekFirstAnnouncementBetween(from uint64, to uint64) (uint64, error) {
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return 0, errors.Wrap(err, "cannot get stream info")
	}

	if to == 0 {
		to = info.State.LastSeq
	}

	if from > to {
		return 0, ErrNotFound
	}

	if info.State.LastSeq == 0 {
		return 0, ErrNotFound
	}

	if from > info.State.LastSeq {
		return 0, ErrNotFound
	}

	// To not cross the stream's boundaries
	if to > info.State.LastSeq {
		to = info.State.LastSeq
	}

	for seq := from; seq <= to; seq++ {
		d, err := p.stream.Get(seq)
		if err != nil {
			return 0, errors.Wrap(err, "cannot get message at the given sequence")
		}

		message, err := v3.ProtoToMessage(d.Data)
		if err != nil {
			return 0, errors.Wrap(err, "cannot decode message at the given sequence")
		}

		switch message.(type) {
		case *messages.BlockAnnouncement:
			return seq, nil
		}
	}

	return 0, ErrNotFound
}

func NewStreamSeek(streamInterface stream.Interface) *StreamSeek {
	return &StreamSeek{
		stream: streamInterface,
	}
}
