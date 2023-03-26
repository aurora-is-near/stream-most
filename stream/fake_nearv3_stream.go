package stream

import (
	"errors"
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/domain/new_format"
	"github.com/aurora-is-near/stream-most/support"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"time"
)

type FakeNearV3Stream struct {
	stream []messages.AbstractNatsMessage
}

func (s FakeNearV3Stream) GetStream() *Stream {
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
		data := support.BuildMessageToRawStreamMsg(msg)
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

	message, err := new_format.ProtoDecode(data)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	switch k := message.Payload.(type) {
	case *borealisproto.Message_NearBlockHeader:
		m.Announcement = messages.NewBlockAnnouncement(k)
	case *borealisproto.Message_NearBlockShard:
		m.Shard = messages.NewBlockShard(k)
	}

	s.stream = append(s.stream, m)
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
