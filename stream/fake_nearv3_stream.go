package stream

import (
	"errors"
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/u"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"time"
)

type FakeNearV3Stream struct {
	stream []messages.AbstractNatsMessage

	deduplicate         bool
	deduplicationHashes map[string]struct{}
}

func (s *FakeNearV3Stream) WithDeduplication() *FakeNearV3Stream {
	s.deduplicate = true
	s.deduplicationHashes = make(map[string]struct{})
	return s
}

func (s *FakeNearV3Stream) IsFake() bool {
	return true
}

func (s *FakeNearV3Stream) GetStream() *Stream {
	return nil
}

func (s *FakeNearV3Stream) Disconnect() error {
	return nil
}

func (s *FakeNearV3Stream) GetInfo(_ time.Duration) (*nats.StreamInfo, time.Time, error) {
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

func (s *FakeNearV3Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
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

func (s *FakeNearV3Stream) Add(msgs ...messages.NatsMessage) {
	for _, msg := range msgs {
		data := u.BuildMessageToRawStreamMsg(msg)
		_, err := s.Write(data.Data, data.Header, nats.AckWait(0))
		if err != nil {
			logrus.Error(err)
		}
	}
}

func (s *FakeNearV3Stream) Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error) {
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
		m.Announcement = messages.NewBlockAnnouncement(k)
	case *borealisproto.Message_NearBlockShard:
		m.Shard = messages.NewBlockShard(k)
	}

	s.stream = append(s.stream, m)
	if s.deduplicate {
		s.deduplicationHashes[header.Get(nats.MsgIdHdr)] = struct{}{}
	}
	return nil, nil
}

func (s *FakeNearV3Stream) Stats() *nats.Statistics {
	return &nats.Statistics{
		InMsgs:     0,
		OutMsgs:    0,
		InBytes:    0,
		OutBytes:   0,
		Reconnects: 0,
	}
}

func (s *FakeNearV3Stream) Display() {
	for _, msg := range s.stream {
		if msg.IsAnnouncement() {
			fmt.Printf("%s:%s ", msg.GetType().String(), msg.GetAnnouncement().Block.Hash[:3])
		} else {
			fmt.Printf("%s:%s:%d ", msg.GetType().String(), msg.GetShard().Block.Hash[:3], msg.GetShard().ShardID)
		}
	}
	fmt.Println()
}

func (s *FakeNearV3Stream) DisplayRows() {
	for _, msg := range s.stream {
		if msg.IsAnnouncement() {
			fmt.Printf("%s:%s\n", msg.GetType().String(), msg.GetAnnouncement().Block.Hash[:3])
		} else {
			fmt.Printf("%s:%s:%d\n", msg.GetType().String(), msg.GetShard().Block.Hash[:3], msg.GetShard().ShardID)
		}
	}
}

func (s *FakeNearV3Stream) DisplayWithHeaders() {
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

func NewFakeStream() *FakeNearV3Stream {
	return NewFakeNearV3Stream()
}

func NewFakeNearV3Stream() *FakeNearV3Stream {
	return &FakeNearV3Stream{
		stream: make([]messages.AbstractNatsMessage, 0),
	}
}
