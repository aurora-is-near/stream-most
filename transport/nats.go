package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	logger *logrus.Entry

	config     *NATSConfig
	dialer     *ctxDialer
	connection *nats.Conn

	everConnected     chan struct{}
	everConnectedOnce sync.Once
	closeErr          error
	closed            chan struct{}
}

func (c *NatsConnection) connect(ctx context.Context) error {
	ctxOptions := []natscontext.Option{}
	if c.config.ServerURL != "" {
		ctxOptions = append(ctxOptions, natscontext.WithServerURL(c.config.ServerURL))
	}
	if c.config.Creds != "" {
		ctxOptions = append(ctxOptions, natscontext.WithCreds(c.config.Creds))
	}

	natsCtx, err := natscontext.New(c.config.ContextName, true, ctxOptions...)
	if err != nil {
		return fmt.Errorf("unable to create or load context: %w", err)
	}

	natsOptsList, err := natsCtx.NATSOptions(
		nats.ConnectHandler(func(nc *nats.Conn) {
			c.logger.Infof("Connected [%v]", nc.ConnectedUrl())
			c.everConnectedOnce.Do(func() {
				close(c.everConnected)
			})
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			c.logger.Error(err)
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				c.logger.Warnf("Disconnected due to: %v", err)
			} else {
				c.logger.Infof("Disconnected")
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Infof("Reconnected [%v]", nc.ConnectedUrl())
			c.everConnectedOnce.Do(func() {
				close(c.everConnected)
			})
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			c.closeErr = nc.LastError()
			c.logger.Infof("Connection closed: %v", c.closeErr)
			close(c.closed)
		}),
	)
	if err != nil {
		return fmt.Errorf("unable to get NATS options from NATS context: %w", err)
	}

	// Options will further be modified, but we don't want to modify the config, so we copy it first.
	optsPtr := c.config.Options
	if optsPtr == nil {
		optsPtr = RecommendedNatsOptions()
	}
	optsCopy := *optsPtr

	optsCopy.Servers = processUrlString(natsCtx.ServerURL())
	if err := ApplyNatsOptions(&optsCopy, natsOptsList...); err != nil {
		return err
	}

	c.dialer = newCtxDialer(getUnderlyingDialer(&optsCopy))
	c.dialer.attachContext(ctx)
	defer c.dialer.detachAllContexts()
	optsCopy.CustomDialer = c.dialer

	if c.connection, err = optsCopy.Connect(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}

	c.dialer.detachAllContexts()
	if c.dialer.ctx.Err() != nil {
		c.Drain()
		return context.Cause(c.dialer.ctx)
	}

	return nil
}

func ConnectNATS(config *NATSConfig) (*NatsConnection, error) {
	return ConnectNATSWithContext(context.Background(), config)
}

func ConnectNATSWithContext(ctx context.Context, config *NATSConfig) (*NatsConnection, error) {
	logger := logrus.StandardLogger()
	if config.OverrideLogger != nil {
		logger = config.OverrideLogger
	}

	conn := &NatsConnection{
		logger:        logger.WithField("component", "nats").WithField("nats", config.LogTag),
		config:        config,
		everConnected: make(chan struct{}),
		closed:        make(chan struct{}),
	}

	conn.logger.Info("Connecting to NATS...")
	if err := conn.connect(ctx); err != nil {
		conn.logger.Errorf("Unable to connect to NATS: %v", err)
		return nil, err
	}

	conn.logger.Infof("Connection status: %s", conn.connection.Status().String())
	return conn, nil
}

func (c *NatsConnection) Drain() error {
	defer c.dialer.cancel(errors.New("nats connection is draining"))
	c.logger.Info("Draining NATS connection...")
	c.connection.Drain()
	<-c.closed
	return c.closeErr
}

func (c *NatsConnection) Conn() *nats.Conn {
	return c.connection
}

func (c *NatsConnection) EverConnected() <-chan struct{} {
	return c.everConnected
}

func (c *NatsConnection) CloseError() error {
	select {
	case <-c.closed:
		return c.closeErr
	default:
		return nil
	}
}

func (c *NatsConnection) Closed() <-chan struct{} {
	return c.closed
}
