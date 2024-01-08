package transport

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	logger *logrus.Entry

	config     *NATSConfig
	connection *nats.Conn
	closeErr   error
	closed     chan struct{}
}

func (c *NatsConnection) connect() error {
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
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			c.logger.Error(err)
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				c.logger.Warnf("Disconnected due to: %v, will try reconnecting", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Infof("Reconnected [%v]", nc.ConnectedUrl())
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

	c.connection, err = optsCopy.Connect()
	return err
}

func ConnectNATS(config *NATSConfig) (*NatsConnection, error) {
	logger := logrus.StandardLogger()
	if config.OverrideLogger != nil {
		logger = config.OverrideLogger
	}

	conn := &NatsConnection{
		logger: logger.WithField("component", "nats").WithField("nats", config.LogTag),
		config: config,
		closed: make(chan struct{}),
	}

	conn.logger.Info("Connecting to NATS...")
	if err := conn.connect(); err != nil {
		conn.logger.Errorf("Unable to connect to NATS: %v", err)
		return nil, err
	}

	conn.logger.Info("Connected successfully")
	return conn, nil
}

func (c *NatsConnection) Drain() error {
	c.logger.Info("Draining NATS connection...")
	c.connection.Drain()
	<-c.closed
	return c.closeErr
}

func (c *NatsConnection) Conn() *nats.Conn {
	return c.connection
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
