package stream

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type Interface interface {
	Options() *Options
	Name() string
	IsFake() bool

	Js() jetstream.JetStream
	Stats() *nats.Statistics
	Stream() jetstream.Stream

	GetConfigSubjects(ctx context.Context) ([]string, error)
	GetInfo(ctx context.Context) (*jetstream.StreamInfo, error)
	Get(ctx context.Context, seq uint64) (messages.NatsMessage, error)
	GetLastMsgForSubject(ctx context.Context, subject string) (messages.NatsMessage, error)

	Write(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

	Disconnect() error
}

type Stream struct {
	*logrus.Entry

	options *Options
	nc      *transport.NatsConnection
	js      jetstream.JetStream
	stream  jetstream.Stream
}

func (s *Stream) Options() *Options {
	return s.options
}

func (s *Stream) Name() string {
	return s.options.Stream
}

func (s *Stream) IsFake() bool {
	return false
}

func (s *Stream) Js() jetstream.JetStream {
	return s.js
}

func (s *Stream) Stats() *nats.Statistics {
	stats := s.nc.Conn().Stats()
	return &stats
}

func (s *Stream) Stream() jetstream.Stream {
	return s.stream
}

func (s *Stream) GetConfigSubjects(ctx context.Context) ([]string, error) {
	next := s.Name()
	for {
		tctx, cancel := context.WithTimeout(ctx, s.options.RequestWait)
		str, err := s.js.Stream(tctx, next)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("unable to get stream '%s': %v", next, err)
		}

		if str.CachedInfo().Config.Mirror == nil {
			return str.CachedInfo().Config.Subjects, nil
		}

		next = str.CachedInfo().Config.Mirror.Name
	}
}

func (s *Stream) GetInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, s.options.RequestWait)
	defer cancel()
	return s.stream.Info(tctx)
}

func (s *Stream) Get(ctx context.Context, seq uint64) (messages.NatsMessage, error) {
	tctx, cancel := context.WithTimeout(ctx, s.options.RequestWait)
	defer cancel()
	msg, err := s.stream.GetMsg(tctx, seq)
	if err != nil {
		return nil, err
	}
	return messages.RawStreamMessage{RawStreamMsg: msg}, nil
}

func (s *Stream) GetLastMsgForSubject(ctx context.Context, subject string) (messages.NatsMessage, error) {
	tctx, cancel := context.WithTimeout(ctx, s.options.RequestWait)
	defer cancel()
	msg, err := s.stream.GetLastMsgForSubject(tctx, subject)
	if err != nil {
		return nil, err
	}
	return messages.RawStreamMessage{RawStreamMsg: msg}, nil
}

func (s *Stream) Write(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	tctx, cancel := context.WithTimeout(ctx, s.options.WriteWait)
	defer cancel()
	return s.js.PublishMsg(tctx, msg, append(opts, jetstream.WithExpectStream(s.Name()))...)
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

func Connect(options *Options) (Interface, error) {
	if options.ShouldFake {
		if options.FakeStream != nil {
			return options.FakeStream, nil
		}
		return createFake(), nil
	}

	options = options.WithDefaults()

	s := &Stream{
		Entry: logrus.
			WithField("stream", options.Stream).
			WithField("nats", options.Nats.LogTag),

		options: options,
	}

	s.Infof("Connecting to NATS at %s", options.Nats.OverrideURL)
	var err error
	s.nc, err = transport.ConnectNATS(options.Nats)
	if err != nil {
		err = fmt.Errorf("unable to connect to NATS: %w", err)
		s.Errorf("%v", err)
		return nil, err
	}
	s.Infof("NATS connected")

	s.Infof("Connecting to JetStream")
	s.js, err = jetstream.New(s.nc.Conn())
	if err != nil {
		err = fmt.Errorf("unable to connect to JetStream: %w", err)
		s.Errorf("%v", err)
		s.nc.Drain()
		return nil, err
	}
	s.Infof("JetStream connected")

	s.Infof("Getting stream '%s'", options.Stream)
	streamRequestCtx, cancelStreamRequest := context.WithTimeout(context.Background(), s.options.RequestWait)
	defer cancelStreamRequest()
	s.stream, err = s.js.Stream(streamRequestCtx, options.Stream)
	if err != nil {
		err = fmt.Errorf("unable to connect to stream: %w", err)
		s.Errorf("%v", err)
		s.nc.Drain()
		return nil, err
	}
	s.Infof("Stream connected")

	return s, nil
}
