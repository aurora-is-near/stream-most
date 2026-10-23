package streamconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type StreamConnector struct {
	logger *logrus.Entry

	cfg    *Config
	nc     *transport.NatsConnection
	js     jetstream.JetStream
	stream *stream.Stream
}

func Connect(cfg *Config) (*StreamConnector, error) {
	return ConnectWithContext(context.Background(), cfg)
}

func ConnectWithContext(ctx context.Context, cfg *Config) (*StreamConnector, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		connectCtx, cancelConnect := context.WithTimeout(ctx, time.Minute)
		defer cancelConnect()
		return ConnectWithContext(connectCtx, cfg)
	}

	sc := &StreamConnector{
		logger: logrus.
			WithField("component", "streamconnector").
			WithField("stream", cfg.Stream.Name).
			WithField("nats", cfg.Nats.LogTag),

		cfg: cfg,
	}

	var err error
	sc.nc, err = transport.ConnectNATSWithContext(ctx, cfg.Nats)
	if err != nil {
		err = fmt.Errorf("unable to connect to NATS: %w", err)
		sc.logger.Errorf("%v", err)
		return nil, err
	}

	select {
	case <-sc.nc.EverConnected():
	default:
		select {
		case <-sc.nc.EverConnected():
		case <-ctx.Done():
			err = fmt.Errorf("can't proceed: NATS is not connected")
			sc.logger.Errorf("%v", err)
			sc.nc.Drain()
			return nil, err
		}
	}

	sc.logger.Infof("Connecting to JetStream...")
	sc.js, err = jetstream.New(sc.nc.Conn())
	if err != nil {
		err = fmt.Errorf("unable to connect to JetStream: %w", err)
		sc.logger.Errorf("%v", err)
		sc.nc.Drain()
		return nil, err
	}
	sc.logger.Infof("JetStream connected")

	sc.stream, err = stream.ConnectWithContext(ctx, cfg.Stream, sc.js)
	if err != nil {
		err = fmt.Errorf("unable to connect stream: %w", err)
		sc.logger.Errorf("%v", err)
		sc.nc.Drain()
		return nil, err
	}

	return sc, nil
}

func (sc *StreamConnector) Stream() *stream.Stream {
	return sc.stream
}

func (sc *StreamConnector) NatsStats() nats.Statistics {
	return sc.nc.Conn().Stats()
}

func (sc *StreamConnector) Disconnect() error {
	if sc.nc == nil {
		return nil
	}
	sc.logger.Info("Disconnecting...")
	err := sc.nc.Drain()
	sc.nc = nil
	return err
}
