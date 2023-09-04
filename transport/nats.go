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
	closed     chan error
}

func (c *NatsConnection) connect() error {
	ctxOptions := []natscontext.Option{}
	if c.config.OverrideURL != "" {
		ctxOptions = append(ctxOptions, natscontext.WithServerURL(c.config.OverrideURL))
	}
	if c.config.OverrideCreds != "" {
		ctxOptions = append(ctxOptions, natscontext.WithCreds(c.config.OverrideCreds))
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
			err := nc.LastError()
			if err != nil {
				c.logger.Infof("Connection closed: %v", err)
			}
			c.closed <- err
		}),
	)
	if err != nil {
		return fmt.Errorf("unable to get NATS options from NATS context: %w", err)
	}

	opts := c.config.Options
	opts.Servers = processUrlString(natsCtx.ServerURL())
	if opts, err = ApplyNatsOptions(opts, natsOptsList...); err != nil {
		return err
	}

	c.connection, err = opts.Connect()
	return err
}

func ConnectNATS(config *NATSConfig) (*NatsConnection, error) {
	conn := &NatsConnection{
		logger: logrus.WithField("component", "nats").WithField("nats", config.LogTag),
		config: config,
		closed: make(chan error, 1),
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
	if err := c.connection.Drain(); err != nil {
		c.logger.Errorf("Error when draining NATS connection: %v", err)
		return err
	}
	return nil
}

func (c *NatsConnection) Conn() *nats.Conn {
	return c.connection
}

func (c *NatsConnection) Closed() <-chan error {
	return c.closed
}
