package fake

import (
	"errors"
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

// Stream is a default fake for a stream.Interface
// It wraps an array and allows for easy behaviour testing,
// but no complex interactions with the stream are testable with it
type Stream struct {
	stream []messages.AbstractNatsMessage

	deduplicate         bool
	deduplicationHashes map[string]struct{}
}

func (s *Stream) GetArray() []messages.AbstractNatsMessage {
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
				Header:   msg.GetMsg().Header,
				Data:     msg.GetMsg().Data,
			}, nil
		}

		i += seq - msg.GetSequence()
	}
	return nil, errors.New("not found in the fake stream")
}

func (s *Stream) Add(msgs ...messages.NatsMessage) {
	for _, msg := range msgs {
		data := u.BuildMessageToRawStreamMsg(msg)
		_, err := s.Write(data.Data, data.Header, nats.AckWait(0))
		if err != nil {
			logrus.Error(err)
		}
	}
}

func (s *Stream) ExpectExactly(t *testing.T, msgs ...messages.NatsMessage) {
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
		if expected.GetBlock().Hash != found.GetBlock().Hash {
			t.Errorf("Different block hashes. Expected: %s, found: %s", expected.GetBlock().Hash, found.GetBlock().Hash)
		}
		if expected.GetBlock().Height != found.GetBlock().Height {
			t.Errorf("Different block heights. Expected: %d, found: %d", expected.GetBlock().Height, found.GetBlock().Height)
		}
		if expected.GetBlock().PrevHash != found.GetBlock().PrevHash {
			t.Errorf("Different block prev hashes. Expected: %s, found: %s", expected.GetBlock().PrevHash, found.GetBlock().PrevHash)
		}
		if expected.IsShard() {
			if expected.GetShard().ShardID != found.GetShard().ShardID {
				t.Errorf("Different shard numbers. Expected: %d, found: %d", expected.GetShard().ShardID, found.GetShard().ShardID)
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
			return nil, nil
		}
	}

	m := messages.NatsMessage{
		Msg: &nats.Msg{Data: data, Header: header},
		Metadata: &nats.MsgMetadata{
			Sequence: nats.SequencePair{
				Consumer: seq,
				Stream:   seq,
			},
			Timestamp: time.Now(),
			Stream:    "fakey-fakey",
		},
		Announcement: nil,
		Shard:        nil,
	}

	message, err := v3.ProtoDecode(data)
	if err != nil {
		logrus.Error("cannot decode proto")
		return nil, err
	}

	switch k := message.Payload.(type) {
	case *borealisproto.Message_NearBlockHeader:
		m.Announcement = messages.NewBlockAnnouncementV3(k)
	case *borealisproto.Message_NearBlockShard:
		m.Shard = messages.NewBlockShard(k)
	}

	s.stream = append(s.stream, m)
	if s.deduplicate {
		s.deduplicationHashes[header.Get(nats.MsgIdHdr)] = struct{}{}
	}
	return nil, nil
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
		if msg.IsAnnouncement() {
			fmt.Printf("%s:%s ", msg.GetType().String(), msg.GetAnnouncement().Block.Hash[:3])
		} else {
			fmt.Printf("%s:%s:%d ", msg.GetType().String(), msg.GetShard().Block.Hash[:3], msg.GetShard().ShardID)
		}
	}
	fmt.Println()
}

func (s *Stream) DisplayRows() {
	for _, msg := range s.stream {
		if msg.IsAnnouncement() {
			fmt.Printf("%s:%s\n", msg.GetType().String(), msg.GetAnnouncement().Block.Hash[:3])
		} else {
			fmt.Printf("%s:%s:%d\n", msg.GetType().String(), msg.GetShard().Block.Hash[:3], msg.GetShard().ShardID)
		}
	}
}

func (s *Stream) DisplayWithHeaders() {
	for _, msg := range s.stream {
		if msg.IsAnnouncement() {
			fmt.Printf(
				"%s:%s [%v]\n",
				msg.GetType().String(),
				msg.GetAnnouncement().Block.Hash[:3],
				msg.GetMsg().Header,
			)
		} else {
			fmt.Printf(
				"%s:%s:%d [%v]\n",
				msg.GetType().String(),
				msg.GetShard().Block.Hash[:3],
				msg.GetShard().ShardID,
				msg.GetMsg().Header,
			)
		}
	}
}

func NewStream() *Stream {
	return &Stream{
		stream: make([]messages.AbstractNatsMessage, 0),
	}
}

func NewFakeStream() stream.Interface {
	return NewStream()
}

func NewFakeStreamWithOptions(_ *stream.Options) stream.Interface {
	return NewStream()
}
