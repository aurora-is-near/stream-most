package fake

import (
	"errors"
	"fmt"
	"testing"
	"time"

	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Stream is a default fake for a stream.Interface
// It wraps an array and allows for easy behaviour testing,
// but no complex interactions with the stream are testable with it
type Stream struct {
	stream []messages.Message

	deduplicate         bool
	deduplicationHashes map[string]struct{}
}

func (s *Stream) GetArray() []messages.Message {
	return s.stream
}

func (s *Stream) Js() nats.JetStreamContext {
	return nil
}

func (s *Stream) Options() *stream.Options {
	return nil
}

func (s *Stream) WithDeduplication() *Stream {
	s.deduplicate = true
	s.deduplicationHashes = make(map[string]struct{})
	return s
}

func (s *Stream) IsFake() bool {
	return true
}

func (s *Stream) GetStream() *stream.Stream {
	return nil
}

func (s *Stream) Disconnect() error {
	return nil
}

func (s *Stream) GetInfo(_ time.Duration) (*nats.StreamInfo, time.Time, error) {
	var firstSeq, lastSeq uint64
	if len(s.stream) > 0 {
		firstSeq = s.stream[0].GetSequence()
		lastSeq = s.stream[len(s.stream)-1].GetSequence()
	}

	return &nats.StreamInfo{
		Created: time.Now().Add(-1337 * time.Hour),
		State: nats.StreamState{
			FirstSeq: firstSeq,
			LastSeq:  lastSeq,
		},
	}, time.Now(), nil
}

func (s *Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
	i := uint64(0)
	for i < uint64(len(s.stream)) {
		msg := s.stream[i]
		if msg.GetSequence() == seq {
			return &nats.RawStreamMsg{
				Sequence: msg.GetSequence(),
				Header:   msg.GetHeader(),
				Data:     msg.GetData(),
			}, nil
		}

		i += seq - msg.GetSequence()
	}
	return nil, errors.New("not found in the fake stream")
}

func (s *Stream) Add(msgs ...messages.Message) {
	for _, msg := range msgs {
		data := u.BuildMessageToRawStreamMsg(msg)
		_, err := s.Write(data.Data, data.Header, nats.AckWait(0))
		if err != nil {
			logrus.Error(err)
		}
	}
}

func (s *Stream) ExpectExactly(t *testing.T, msgs ...messages.Message) {
	if len(msgs) != len(s.stream) {
		t.Errorf("Different count of messages. Expected: %d, found: %d", len(msgs), len(s.stream))
		return
	}

	for i := 0; i < len(msgs); i++ {
		expected := msgs[i]
		found := s.stream[i]
		if expected.GetType() != found.GetType() {
			t.Errorf("Different message types. Expected: %d, found: %d", expected.GetType(), found.GetType())
		}
		if expected.GetSequence() != found.GetSequence() {
			t.Errorf("Different message sequences. Expected: %d, found: %d", expected.GetSequence(), found.GetSequence())
		}
		if expected.GetHash() != found.GetHash() {
			t.Errorf("Different block hashes. Expected: %s, found: %s", expected.GetHash(), found.GetHash())
		}
		if expected.GetHeight() != found.GetHeight() {
			t.Errorf("Different block heights. Expected: %d, found: %d", expected.GetHeight(), found.GetHeight())
		}
		if expected.GetPrevHash() != found.GetPrevHash() {
			t.Errorf("Different block prev hashes. Expected: %s, found: %s", expected.GetPrevHash(), found.GetPrevHash())
		}
		if expected.GetType() != found.GetType() {
			t.Errorf("Different block types. Expected: %s, found: %s", expected.GetType().String(), found.GetType().String())
		}
		if expected.GetType() == messages.Shard {
			expectedShardID := expected.GetShard().GetShardID()
			foundShardID := found.GetShard().GetShardID()
			if expectedShardID != foundShardID {
				t.Errorf("Different shard numbers. Expected: %d, found: %d", expectedShardID, foundShardID)
			}
		}
	}
}

func (s *Stream) Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error) {
	seq := uint64(1)
	if len(s.stream) != 0 {
		seq = s.stream[len(s.stream)-1].GetSequence() + 1
	}

	if s.deduplicate {
		if _, ok := s.deduplicationHashes[header.Get(nats.MsgIdHdr)]; ok {
			return &nats.PubAck{
				Stream:    "fakey-fakey",
				Sequence:  seq,
				Duplicate: true,
				Domain:    "",
			}, nil
		}
	}

	block, err := v3.DecodeProtoBlock(data)
	if err != nil {
		return nil, err
	}
	m := &messages.AbstractMessage{
		TypedMessage: messages.TypedMessage{
			Block: block,
		},
		Msg: &nats.Msg{
			Data:   data,
			Header: header,
		},
		Meta: &nats.MsgMetadata{
			Sequence: nats.SequencePair{
				Stream: seq,
			},
			Timestamp: time.Now(),
			Stream:    "fakey-fakey",
		},
	}

	s.stream = append(s.stream, m)
	if s.deduplicate {
		s.deduplicationHashes[header.Get(nats.MsgIdHdr)] = struct{}{}
	}
	return &nats.PubAck{
		Stream:    "fakey-fakey",
		Sequence:  seq,
		Duplicate: false,
	}, nil
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

func (s *Stream) Display() {
	for _, msg := range s.stream {
		if msg.GetType() == messages.Shard {
			fmt.Printf("%s:%s:%d ", msg.GetType().String(), msg.GetHash()[:3], msg.GetShard().GetShardID())
		} else {
			fmt.Printf("%s:%s ", msg.GetType().String(), msg.GetHash()[:3])
		}
	}
	fmt.Println()
}

func (s *Stream) DisplayRows() {
	for _, msg := range s.stream {
		if msg.GetType() == messages.Shard {
			fmt.Printf("%s:%s:%d\n", msg.GetType().String(), msg.GetHash()[:3], msg.GetShard().GetShardID())
		} else {
			fmt.Printf("%s:%s\n", msg.GetType().String(), msg.GetHash()[:3])
		}
	}
}

func (s *Stream) DisplayWithHeaders() {
	for _, msg := range s.stream {
		if msg.GetType() == messages.Shard {
			fmt.Printf(
				"%s:%s:%d [%v]\n",
				msg.GetType().String(),
				msg.GetHash()[:3],
				msg.GetShard().GetShardID(),
				msg.GetHeader(),
			)
		} else {
			fmt.Printf(
				"%s:%s: [%v]\n",
				msg.GetType().String(),
				msg.GetHash()[:3],
				msg.GetHeader(),
			)
		}
	}
}

func NewStream() *Stream {
	return &Stream{
		stream: make([]messages.Message, 0),
	}
}

func NewFakeStream() stream.Interface {
	return NewStream()
}

func NewFakeStreamWithOptions(_ *stream.Options) stream.Interface {
	return NewStream()
}
