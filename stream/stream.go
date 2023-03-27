package stream

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
)

type Opts struct {
	Nats          *transport.NatsConnectionConfig
	Stream        string
	Subject       string `json:",omitempty"`
	RequestWaitMs uint

	ShouldFake bool
	FakeStream Interface
}

type Stream struct {
	Opts        *Opts
	requestWait nats.MaxWait
	nc          *transport.NatsConnection
	Js          nats.JetStreamContext

	info     *nats.StreamInfo
	infoErr  error
	infoTime time.Time
	infoMtx  sync.Mutex
}

func (opts Opts) FillMissingFields() *Opts {
	if opts.RequestWaitMs == 0 {
		opts.RequestWaitMs = 5000
	}
	return &opts
}

type Interface interface {
	GetStream() *Stream
	Disconnect() error
	GetInfo(ttl time.Duration) (*nats.StreamInfo, time.Time, error)
	Get(seq uint64) (*nats.RawStreamMsg, error)
	Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error)
	Stats() *nats.Statistics
	IsFake() bool
}

func ConnectStream(opts *Opts) (Interface, error) {
	if opts.ShouldFake {
		if opts.FakeStream != nil {
			return opts.FakeStream, nil
		} else {
			return &FakeNearV3Stream{}, nil
		}
	}

	opts = opts.FillMissingFields()
	s := &Stream{
		Opts:        opts,
		requestWait: nats.MaxWait(time.Millisecond * time.Duration(opts.RequestWaitMs)),
	}

	s.log("connecting to NATS...")
	var err error
	s.nc, err = transport.ConnectNATS(opts.Nats, nil)
	if err != nil {
		s.log("unable to connect to NATS: %v", err)
		s.Disconnect()
		return nil, err
	}

	s.log("connecting to NATS JetStream...")
	s.Js, err = s.nc.Conn().JetStream(s.requestWait)
	if err != nil {
		s.log("unable to connect to NATS JetStream: %v", err)
		s.Disconnect()
		return nil, err
	}

	s.log("getting stream info...")
	info, _, err := s.GetInfo(0)
	if err != nil {
		s.log("unable to get stream info: %v", err)
		s.Disconnect()
		return nil, err
	}

	if len(opts.Subject) == 0 {
		s.log("subject is not specified, figuring it out automatically...")
		curInfo := info
		for curInfo.Config.Mirror != nil {
			mirrorName := curInfo.Config.Mirror.Name
			s.log("stream '%s' is mirrored from stream '%s', getting it's info...", curInfo.Config.Name, mirrorName)
			curInfo, err = s.Js.StreamInfo(mirrorName, s.requestWait)
			if err != nil {
				s.log("unable to get stream '%s' info: %v", mirrorName, err)
				s.Disconnect()
				return nil, err
			}
		}

		if len(curInfo.Config.Subjects) == 0 {
			err := fmt.Errorf("stream '%s' has no subjects", curInfo.Config.Name)
			s.log(err.Error())
			s.Disconnect()
			return nil, err
		}

		opts.Subject = curInfo.Config.Subjects[0]
		s.log("subject '%s' is chosen", opts.Subject)
	}

	s.log("connected")

	return s, nil
}

func (s *Stream) IsFake() bool {
	return false
}

func (s *Stream) GetStream() *Stream {
	return s
}

func (s *Stream) Disconnect() error {
	if s.nc == nil {
		return nil
	}
	s.log("disconnecting...")
	err := s.nc.Drain()
	s.nc = nil
	return err
}

func (s *Stream) GetInfo(ttl time.Duration) (*nats.StreamInfo, time.Time, error) {
	s.infoMtx.Lock()
	defer s.infoMtx.Unlock()
	if ttl == 0 || time.Since(s.infoTime) > ttl {
		s.info, s.infoErr = s.Js.StreamInfo(s.Opts.Stream, s.requestWait)
		s.infoTime = time.Now()
	}
	return s.info, s.infoTime, s.infoErr
}

func (s *Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
	return s.Js.GetMsg(s.Opts.Stream, seq, s.requestWait)
}

func (s *Stream) Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error) {
	header.Add(nats.ExpectedStreamHdr, s.Opts.Stream)
	msg := &nats.Msg{
		Subject: s.Opts.Subject,
		Header:  header,
		Data:    data,
	}
	return s.Js.PublishMsg(msg, publishAckWait)
}

func (s *Stream) log(format string, v ...any) {
	log.Printf(fmt.Sprintf("Stream [ss / %s]: ", s.Opts.Nats.LogTag, s.Opts.Stream)+format, v...)
}

func (s *Stream) Stats() *nats.Statistics {
	stats := s.nc.Conn().Stats()
	return &stats
}
