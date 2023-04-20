package stream_seek

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrEmptyStream = errors.New("empty stream")
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

		message, err := formats.Active().ParseRawMsg(d)
		if err != nil {
			return shards, err
		}

		if message.IsShard() {
			if forBlock == nil || (message.GetBlock().Hash == *forBlock) {
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
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return 0, p.wrapNatsError(err)
	}

	if info.State.LastSeq == 0 {
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

		d, err := p.stream.Get(seq)
		if err != nil {
			if errors.Is(err, nats.ErrMsgNotFound) {
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

		if len(d.Data) == 0 {
			// Empty message :(
			shift += 1
			continue
		}

		message, err := formats.Active().ParseRawMsg(d)
		if err != nil {
			logrus.Info("Corrupted message")
			return 0, err
		}

		switch {
		case message.IsAnnouncement():
			shift = 0
			if message.GetBlock().Height < height {
				l = (l + r) / 2
			} else {
				r = (l + r) / 2
			}
		case message.IsShard():
			shift += 1
		}
	}

	for shift := uint64(0); shift < 10; shift++ {
		d, err := p.stream.Get(l + shift)
		if err != nil {
			logrus.Error(errors.Wrap(err, "cannot read message:"))
			continue
		}

		message, err := formats.Active().ParseRawMsg(d)
		if err != nil {
			logrus.Error(errors.Wrap(err, "corrupted message:"))
			continue
		}

		switch {
		case message.IsAnnouncement():
			logrus.Infof("Found block announcement with height %d at sequence %d", message.GetBlock().Height, l)
			return l + shift, nil
		}
	}

	return 0, ErrNotFound
}

func (p *StreamSeek) SeekLastFullyWrittenBlock() (
	announcement messages.AbstractNatsMessage,
	shards []messages.AbstractNatsMessage,
	err error,
) {
	info, _, err := p.stream.GetInfo(0)
	if err != nil {
		return nil, nil, p.wrapNatsError(err)
	}

	if info.State.LastSeq == 0 {
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

	var shardsStash []messages.AbstractNatsMessage

	for seq := upperBound; seq >= lowerBound; seq-- {
		msg, err := p.stream.Get(seq)
		if err != nil {
			return nil, nil, p.wrapNatsError(err)
		}

		// We need to inspect the message to see its height
		message, err := formats.Active().ParseRawMsg(msg)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case message.IsAnnouncement():
			countOfShards := 0
			for _, v := range message.GetAnnouncement().ParticipatingShardsMap {
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
		case message.IsShard():
			// We found some shard, stash it
			shardsStash = append(shardsStash, message)
		}
	}

	return nil, nil, ErrNotFound
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
		d, err := p.stream.Get(seq)
		if err != nil {
			if errors.Is(err, nats.ErrMsgNotFound) {
				logrus.Warn("Stream is deleting messages too fast, skipping 2%")
				seq += (to - from) / 50
				continue
			}
			return 0, errors.Wrap(err, "cannot get message at the given sequence")
		}

		message, err := formats.Active().ParseRawMsg(d)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Those are truncated messages, just skip
				logrus.Warn(errors.Wrap(err, "cannot decode message at the given sequence"))
				continue
			}
			return 0, errors.Wrap(err, "cannot decode message at the given sequence")
		}

		if message.IsAnnouncement() {
			return seq, nil
		}
	}

	return 0, ErrNotFound
}

func (p *StreamSeek) wrapNatsError(err error) error {
	if errors.Is(err, nats.ErrMsgNotFound) {
		return ErrNotFound
	}

	return errors.Wrap(err, "stream seek: ")
}

func NewStreamSeek(streamInterface stream.Interface) *StreamSeek {
	return &StreamSeek{
		stream: streamInterface,
	}
}
