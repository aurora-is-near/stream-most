package stream

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-most/transport"
)

type Interface interface {
	Options() *Options
	Js() nats.JetStreamContext
	Disconnect() error
	GetInfo(ttl time.Duration) (*nats.StreamInfo, time.Time, error)
	Get(seq uint64) (*nats.RawStreamMsg, error)
	Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error)
	Stats() *nats.Statistics
	IsFake() bool
}

type Stream struct {
	*logrus.Entry

	options     *Options
	requestWait nats.MaxWait
	nc          *transport.NatsConnection
	js          nats.JetStreamContext

	info     *nats.StreamInfo
	infoErr  error
	infoTime time.Time
	infoMtx  sync.Mutex
}

func (s *Stream) Options() *Options {
	return s.options
}

func (s *Stream) Js() nats.JetStreamContext {
	return s.js
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
	s.Info("Disconnecting...")
	err := s.nc.Drain()
	s.nc = nil
	return err
}

func (s *Stream) GetInfo(ttl time.Duration) (*nats.StreamInfo, time.Time, error) {
	s.infoMtx.Lock()
	defer s.infoMtx.Unlock()
	if ttl == 0 || time.Since(s.infoTime) > ttl {
		s.info, s.infoErr = s.js.StreamInfo(s.options.Stream, s.requestWait)
		s.infoTime = time.Now()
	}
	return s.info, s.infoTime, s.infoErr
}

func (s *Stream) Get(seq uint64) (*nats.RawStreamMsg, error) {
	return s.js.GetMsg(s.options.Stream, seq, s.requestWait)
}

func (s *Stream) Write(data []byte, header nats.Header, publishAckWait nats.AckWait) (*nats.PubAck, error) {
	header.Add(nats.ExpectedStreamHdr, s.options.Stream)
	msg := &nats.Msg{
		Subject: s.options.Subject,
		Header:  header,
		Data:    data,
	}
	return s.js.PublishMsg(msg, publishAckWait)
}

func (s *Stream) Stats() *nats.Statistics {
	stats := s.nc.Conn().Stats()
	return &stats
}

func newStream(options *Options) (Interface, error) {
	s := &Stream{
		Entry: logrus.WithField("stream", options.Stream).
			WithField("subject", options.Subject).
			WithField("log_tag", options.Nats.LogTag),

		options:     options,
		requestWait: nats.MaxWait(time.Millisecond * time.Duration(options.RequestWaitMs)),
	}

	s.Infof("Connecting to NATS at %v", options.Nats.Endpoints)
	var err error
	s.nc, err = transport.NewConnection(options.Nats.WithDefaults(), nil)
	if err != nil {
		s.Errorf("Unable to connect to NATS: %v", err)
		_ = s.Disconnect()
		return nil, err
	}

	s.Info("Connecting to JetStream")
	s.js, err = s.nc.Conn().JetStream(s.requestWait)
	if err != nil {
		s.Infof("Unable to connect to NATS JetStream: %v", err)
		_ = s.Disconnect()
		return nil, err
	}

	s.Info("Getting stream info...")
	info, _, err := s.GetInfo(0)
	if err != nil {
		s.Infof("Unable to get stream info: %v", err)
		_ = s.Disconnect()
		return nil, err
	}

	if len(options.Subject) == 0 {
		s.Info("Subject is not specified, figuring it out automatically...")
		curInfo := info
		for curInfo.Config.Mirror != nil {
			mirrorName := curInfo.Config.Mirror.Name
			s.Infof("streamOpts '%s' is mirrored from stream '%s', getting it's info...", curInfo.Config.Name, mirrorName)
			curInfo, err = s.js.StreamInfo(mirrorName, s.requestWait)
			if err != nil {
				s.Infof("unable to get stream '%s' info: %v", mirrorName, err)
				_ = s.Disconnect()
				return nil, err
			}
		}

		if len(curInfo.Config.Subjects) == 0 {
			err := fmt.Errorf("stream '%s' has no subjects", curInfo.Config.Name)
			s.Error(err)
			_ = s.Disconnect()
			return nil, err
		}

		options.Subject = curInfo.Config.Subjects[0]
		s.Infof("Subject '%s' is chosen", options.Subject)
	}

	s.Info("Connected")

	return s, nil
}
