package stream

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type Stream struct {
	logger *logrus.Entry

	cfg    *Config
	js     jetstream.JetStream
	stream jetstream.Stream
}

func Connect(cfg *Config, js jetstream.JetStream) (*Stream, error) {
	return ConnectWithContext(context.Background(), cfg, js)
}

func ConnectWithContext(ctx context.Context, cfg *Config, js jetstream.JetStream) (*Stream, error) {
	cfg = cfg.WithDefaults()

	s := &Stream{
		logger: logrus.
			WithField("component", "stream").
			WithField("stream", cfg.Name).
			WithField("tag", cfg.LogTag),

		cfg: cfg,
		js:  js,
	}

	s.logger.Infof("Getting stream '%s'", cfg.Name)
	streamRequestCtx, cancelStreamRequest := context.WithTimeout(ctx, s.cfg.RequestWait)
	defer cancelStreamRequest()

	var err error
	s.stream, err = s.js.Stream(streamRequestCtx, cfg.Name)
	if err != nil {
		err = fmt.Errorf("unable to connect to stream: %w", err)
		s.logger.Errorf("%v", err)
		return nil, err
	}
	s.logger.Infof("Stream connected")

	return s, nil
}

func (s *Stream) Name() string {
	return s.cfg.Name
}

func (s *Stream) Js() jetstream.JetStream {
	return s.js
}

func (s *Stream) Stream() jetstream.Stream {
	return s.stream
}

func (s *Stream) GetConfigSubjects(ctx context.Context) ([]string, error) {
	next := s.Name()
	for {
		tctx, cancel := context.WithTimeout(ctx, s.cfg.RequestWait)
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
	tctx, cancel := context.WithTimeout(ctx, s.cfg.RequestWait)
	defer cancel()
	return s.stream.Info(tctx)
}

func (s *Stream) Get(ctx context.Context, seq uint64) (messages.NatsMessage, error) {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.RequestWait)
	defer cancel()
	msg, err := s.stream.GetMsg(tctx, seq)
	if err != nil {
		return nil, err
	}
	return messages.RawStreamMessage{RawStreamMsg: msg}, nil
}

func (s *Stream) GetLastMsgForSubject(ctx context.Context, subject string) (messages.NatsMessage, error) {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.RequestWait)
	defer cancel()
	msg, err := s.stream.GetLastMsgForSubject(tctx, subject)
	if err != nil {
		return nil, err
	}
	return messages.RawStreamMessage{RawStreamMsg: msg}, nil
}

func (s *Stream) OrderedConsumer(ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.RequestWait)
	defer cancel()
	return s.stream.OrderedConsumer(tctx, cfg)
}

func (s *Stream) Write(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.WriteWait)
	defer cancel()
	return s.js.PublishMsg(tctx, msg, append(opts, jetstream.WithExpectStream(s.Name()))...)
}
