package fake

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

// Stream is a default fake for a stream.Interface
// It wraps an array and allows for easy behaviour testing,
// but no complex interactions with the stream are testable with it
type Stream struct {
	stream []*messages.BlockMessage

	deduplicate         bool
	deduplicationHashes map[string]uint64
}

func (s *Stream) Options() *stream.Options {
	return nil
}

func (s *Stream) Name() string {
	return "fakey-fakey"
}

func (s *Stream) IsFake() bool {
	return true
}

func (s *Stream) Js() jetstream.JetStream {
	return nil
}

func (s *Stream) Stats() *nats.Statistics {
	return &nats.Statistics{
		InMsgs:     0,
		OutMsgs:    0,
		InBytes:    0,
		OutBytes:   0,
		Reconnects: 0,
	}
}

func (s *Stream) Stream() jetstream.Stream {
	return nil
}

func (s *Stream) GetConfigSubjects(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("not implemented for fake stream")
}

func (s *Stream) GetInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	var firstSeq, lastSeq uint64
	if len(s.stream) > 0 {
		firstSeq = s.stream[0].Msg.GetSequence()
		lastSeq = s.stream[len(s.stream)-1].Msg.GetSequence()
	}

	return &jetstream.StreamInfo{
		Created: time.Now().Add(-1337 * time.Hour),
		State: jetstream.StreamState{
			Msgs:     uint64(len(s.stream)),
			FirstSeq: firstSeq,
			LastSeq:  lastSeq,
		},
	}, nil
}

func (s *Stream) Get(ctx context.Context, seq uint64) (messages.NatsMessage, error) {
	i := uint64(0)
	for i < uint64(len(s.stream)) {
		msg := s.stream[i]
		if msg.Msg.GetSequence() == seq {
			return msg.Msg, nil
		}
		i += seq - msg.Msg.GetSequence()
	}
	return nil, errors.New("not found in the fake stream")
}

func (s *Stream) GetLastMsgForSubject(ctx context.Context, subject string) (messages.NatsMessage, error) {
	return nil, fmt.Errorf("not implemented for fake stream")
}

func (s *Stream) Write(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	seq := uint64(1)
	if len(s.stream) != 0 {
		seq = s.stream[len(s.stream)-1].Msg.GetSequence() + 1
	}

	header := make(nats.Header)
	for k, v := range msg.Header {
		vCopy := make([]string, len(v))
		copy(vCopy, v)
		header[k] = vCopy
	}

	applyPublishOpts(header, opts...)

	if s.deduplicate {
		// TODO: check if expectations align
		if dupSeq, ok := s.deduplicationHashes[msg.Header.Get(jetstream.MsgIDHeader)]; ok {
			return &jetstream.PubAck{
				Stream:    s.Name(),
				Sequence:  dupSeq,
				Duplicate: true,
			}, nil
		}
	}

	block, err := v3.DecodeProtoBlock(msg.Data)
	if err != nil {
		return nil, err
	}
	m := &messages.BlockMessage{
		Block: block,
		Msg: messages.RawStreamMessage{
			RawStreamMsg: &jetstream.RawStreamMsg{
				Subject:  msg.Subject,
				Sequence: seq,
				Header:   header,
				Data:     msg.Data,
				Time:     time.Now(),
			},
		},
	}

	s.stream = append(s.stream, m)
	if s.deduplicate {
		s.deduplicationHashes[msg.Header.Get(jetstream.MsgIDHeader)] = seq
	}
	return &jetstream.PubAck{
		Stream:   s.Name(),
		Sequence: seq,
	}, nil
}

func (s *Stream) Disconnect() error {
	return nil
}

func (s *Stream) GetArray() []*messages.BlockMessage {
	return s.stream
}

func (s *Stream) WithDeduplication() *Stream {
	s.deduplicate = true
	s.deduplicationHashes = make(map[string]uint64)
	return s
}

func (s *Stream) Add(msgs ...*messages.BlockMessage) {
	for _, msg := range msgs {
		_, err := s.Write(
			context.Background(),
			&nats.Msg{
				Subject: msg.Msg.GetSubject(),
				Header:  msg.Msg.GetHeader(),
				Data:    msg.Msg.GetData(),
			},
		)
		if err != nil {
			logrus.Error(err)
		}
	}
}

func (s *Stream) ExpectExactly(t *testing.T, msgs ...*messages.BlockMessage) {
	if len(msgs) != len(s.stream) {
		t.Errorf("Different count of messages. Expected: %d, found: %d", len(msgs), len(s.stream))
		return
	}

	for i := 0; i < len(msgs); i++ {
		expected := msgs[i]
		found := s.stream[i]
		if expected.Block.GetBlockType() != found.Block.GetBlockType() {
			t.Errorf("Different message types. Expected: %d, found: %d", expected.Block.GetBlockType(), found.Block.GetBlockType())
		}
		if expected.Msg.GetSequence() != found.Msg.GetSequence() {
			t.Errorf("Different message sequences. Expected: %d, found: %d", expected.Msg.GetSequence(), found.Msg.GetSequence())
		}
		if expected.Block.GetHash() != found.Block.GetHash() {
			t.Errorf("Different block hashes. Expected: %s, found: %s", expected.Block.GetHash(), found.Block.GetHash())
		}
		if expected.Block.GetHeight() != found.Block.GetHeight() {
			t.Errorf("Different block heights. Expected: %d, found: %d", expected.Block.GetHeight(), found.Block.GetHeight())
		}
		if expected.Block.GetPrevHash() != found.Block.GetPrevHash() {
			t.Errorf("Different block prev hashes. Expected: %s, found: %s", expected.Block.GetPrevHash(), found.Block.GetPrevHash())
		}
		if expected.Block.GetBlockType() == blocks.Shard {
			expectedShardID := expected.Block.GetShardID()
			foundShardID := found.Block.GetShardID()
			if expectedShardID != foundShardID {
				t.Errorf("Different shard numbers. Expected: %d, found: %d", expectedShardID, foundShardID)
			}
		}
	}
}

func (s *Stream) Display() {
	for _, msg := range s.stream {
		if msg.Block.GetBlockType() == blocks.Shard {
			fmt.Printf("%s:%s:%d ", msg.Block.GetBlockType().String(), msg.Block.GetHash()[:3], msg.Block.GetShardID())
		} else {
			fmt.Printf("%s:%s ", msg.Block.GetBlockType().String(), msg.Block.GetHash()[:3])
		}
	}
	fmt.Println()
}

func (s *Stream) DisplayRows() {
	for _, msg := range s.stream {
		if msg.Block.GetBlockType() == blocks.Shard {
			fmt.Printf("%s:%s:%d\n", msg.Block.GetBlockType().String(), msg.Block.GetHash()[:3], msg.Block.GetShardID())
		} else {
			fmt.Printf("%s:%s\n", msg.Block.GetBlockType().String(), msg.Block.GetHash()[:3])
		}
	}
}

func (s *Stream) DisplayWithHeaders() {
	for _, msg := range s.stream {
		if msg.Block.GetBlockType() == blocks.Shard {
			fmt.Printf(
				"%s:%s:%d [%v]\n",
				msg.Block.GetBlockType().String(),
				msg.Block.GetHash()[:3],
				msg.Block.GetShardID(),
				msg.Msg.GetHeader(),
			)
		} else {
			fmt.Printf(
				"%s:%s: [%v]\n",
				msg.Block.GetBlockType().String(),
				msg.Block.GetHash()[:3],
				msg.Msg.GetHeader(),
			)
		}
	}
}

func NewStream() *Stream {
	return &Stream{
		stream: make([]*messages.BlockMessage, 0),
	}
}

func NewFakeStream() stream.Interface {
	return NewStream()
}

func NewFakeStreamWithOptions(_ *stream.Options) stream.Interface {
	return NewStream()
}
