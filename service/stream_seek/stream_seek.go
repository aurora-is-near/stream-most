package stream_seek

import (
	"context"
	"io"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrEmptyStream = errors.New("empty stream")
)

// StreamSeek provides methods to seek for something on the given stream
type StreamSeek struct {
	stream stream.Interface
}

// SeekShards
func (p *StreamSeek) SeekShards(from, to uint64, forBlock *string) ([]*messages.BlockMessage, error) {
	var shards []*messages.BlockMessage

	if from == 0 {
		from = 1
	}

	for seq := to; seq >= from; seq-- {
		d, err := p.stream.Get(context.Background(), seq)
		if err != nil {
			continue
		}

		message, err := formats.Active().ParseMsg(d)
		if err != nil {
			return shards, err
		}

		if message.Block.GetBlockType() == blocks.Shard {
			if forBlock == nil || (message.Block.GetHash() == *forBlock) {
				shards = append(shards, message)
			}
		}
	}

	return shards, nil
}

// SeekAnnouncementWithHeightBelow returns the sequence number of the latest block announcement which height
// is below a given one. notBefore and notAfter are given in sequence numbers and are used to limit the search range.
// Set notAfter to 0 to search until the end of the stream.
func (p *StreamSeek) SeekAnnouncementWithHeightBelow(height uint64, notBefore uint64, notAfter uint64) (uint64, error) {
	info, err := p.stream.GetInfo(context.Background())
	if err != nil {
		return 0, p.wrapNatsError(err)
	}

	if info.State.Msgs == 0 || info.State.LastSeq == 0 || info.State.FirstSeq > info.State.LastSeq {
		return 0, p.wrapNatsError(ErrNotFound)
	}

	if notBefore > info.State.LastSeq {
		return 0, ErrNotFound
	}

	// To not cross the stream's boundaries
	if notAfter > info.State.LastSeq {
		notAfter = info.State.LastSeq
	}

	// if notAfter is not specified, we go until the end of the stream
	if notAfter == 0 {
		notAfter = info.State.LastSeq
	}

	// Binary search :)
	// L included in the range of search, R excluded
	l := notBefore
	r := notAfter + 1
	shift := uint64(0)

	for l+1 < r {
		seq := (l+r)/2 + shift
		if seq > notAfter {
			r = (l + r) / 2
			shift = 0
			continue
		}

		logrus.Infof("Searching at sequence %d...", seq)

		d, err := p.stream.Get(context.Background(), seq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				l = (l + r) / 2
				shift = 0
				continue
			}
			if errors.Is(err, io.EOF) {
				r = (l + r) / 2
				shift = 0
				continue
			}
			return 0, p.wrapNatsError(err)
		}

		if len(d.GetData()) == 0 {
			// Empty message :(
			shift++
			continue
		}

		message, err := formats.Active().ParseMsg(d)
		if err != nil {
			logrus.Errorf("Stream seek: corrupted message on seq=%d: %v", seq, err)
			return 0, err
		}

		switch message.Block.GetBlockType() {
		case blocks.Announcement:
			shift = 0
			if message.Block.GetHeight() < height {
				l = (l + r) / 2
			} else {
				r = (l + r) / 2
			}
		case blocks.Shard:
			shift++
		}
	}

	for shift := uint64(0); shift < 10; shift++ {
		d, err := p.stream.Get(context.Background(), l+shift)
		if err != nil {
			logrus.Error(errors.Wrap(err, "cannot read message: "))
			continue
		}

		message, err := formats.Active().ParseMsg(d)
		if err != nil {
			logrus.Error(errors.Wrap(err, "corrupted message: "))
			continue
		}

		switch message.Block.GetBlockType() {
		case blocks.Announcement:
			logrus.Infof("Stream Seek: found block announcement with height %d at sequence %d", message.Block.GetHeight(), l)
			return l + shift, nil
		}
	}

	return 0, ErrNotFound
}

func (p *StreamSeek) SeekLastFullyWrittenBlock() (
	announcement *messages.BlockMessage,
	shards []*messages.BlockMessage,
	err error,
) {
	info, err := p.stream.GetInfo(context.Background())
	if err != nil {
		return nil, nil, p.wrapNatsError(err)
	}

	if info.State.Msgs == 0 || info.State.LastSeq == 0 || info.State.FirstSeq > info.State.LastSeq {
		return nil, nil, ErrEmptyStream
	}

	upperBound := info.State.LastSeq
	lowerBound := info.State.FirstSeq
	if upperBound-lowerBound > 100 {
		lowerBound = upperBound - 100
	}
	if lowerBound == 0 {
		lowerBound = 1
	}

	var shardsStash []*messages.BlockMessage

	for seq := upperBound; seq >= lowerBound; seq-- {
		msg, err := p.stream.Get(context.Background(), seq)
		if err != nil {
			return nil, nil, p.wrapNatsError(err)
		}

		// We need to inspect the message to see its height
		message, err := formats.Active().ParseMsg(msg)
		if err != nil {
			return nil, nil, err
		}

		switch message.Block.GetBlockType() {
		case blocks.Announcement:
			countOfShards := 0
			for _, v := range message.Block.GetShardMask() {
				if v {
					countOfShards++
				}
			}

			if len(shardsStash) != countOfShards {
				shardsStash = nil
				break
			}

			// Reverse the stash
			ln := len(shardsStash)
			for i := 0; i < ln/2; i++ {
				j := ln - i - 1
				shardsStash[i], shardsStash[j] = shardsStash[j], shardsStash[i]
			}

			return message, shardsStash, nil
		case blocks.Shard:
			// We found some shard, stash it
			shardsStash = append(shardsStash, message)
		}
	}

	return nil, nil, ErrNotFound
}

// SeekFirstAnnouncementBetween returns the sequence number of the first block announcement which sequence number
// is between from and to. Both from and to are given in sequence numbers and are used to limit the search range.
// Despite it's name, it doesn't really guarantee that the returned announcement is the first one.
// It might return non-first if NATS for some reason gives a lot of strange not-founds while we are searching.
// For example, it will most likely return non-first announcement if it searches from the beginning of the actively
// written stream with discard policy set to old, and the latency to the NATS cluster is high.
func (p *StreamSeek) SeekFirstAnnouncementBetween(from uint64, to uint64) (uint64, error) {
	info, err := p.stream.GetInfo(context.Background())
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

	if info.State.FirstSeq > from {
		from = info.State.FirstSeq
	}

	if from > info.State.LastSeq {
		return 0, ErrNotFound
	}

	// To not cross the stream's boundaries
	if to > info.State.LastSeq {
		to = info.State.LastSeq
	}

	for seq := from; seq <= to; seq++ {
		logrus.Info("Looking at sequence ", seq)
		d, err := p.stream.Get(context.Background(), seq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				logrus.Warn("Stream is deleting old messages too fast, skipping 2%")
				seq += (to - from) / 50
				continue
			}
			return 0, errors.Wrap(err, "cannot get message at the given sequence")
		}

		message, err := formats.Active().ParseMsg(d)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Those are truncated messages, just skip
				logrus.Warn(errors.Wrap(err, "cannot decode message at the given sequence"))
				continue
			}
			return 0, errors.Wrap(err, "cannot decode message at the given sequence")
		}

		if message.Block.GetBlockType() == blocks.Announcement {
			return seq, nil
		}
	}

	return 0, ErrNotFound
}

func (p *StreamSeek) wrapNatsError(err error) error {
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return ErrNotFound
	}

	return errors.Wrap(err, "stream seek: ")
}

func NewStreamSeek(streamInterface stream.Interface) *StreamSeek {
	return &StreamSeek{
		stream: streamInterface,
	}
}
