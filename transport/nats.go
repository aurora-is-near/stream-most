package transport

import (
	"github.com/sirupsen/logrus"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	*logrus.Entry

	opts       *Options
	connection *nats.Conn
	closed     chan error
	errorChan  chan<- error
}

func (c *NatsConnection) connect() error {
	options := []nats.Option{
		nats.Name(c.opts.Name),
		nats.ReconnectWait(time.Second / 5),
		nats.PingInterval(time.Duration(c.opts.PingIntervalMs) * time.Millisecond),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.MaxPingsOutstanding(c.opts.MaxPingsOutstanding),
		nats.Timeout(time.Duration(c.opts.TimeoutMs) * time.Millisecond),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			c.Error(err)
			if c.errorChan != nil {
				c.errorChan <- err
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				c.Warnf("Disconnected due to: %v, will try reconnecting", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.Infof("Reconnected [%v]", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				c.Infof("Connection closed: %v", err)
			}
			c.closed <- err
		}),
	}
	if len(c.opts.Creds) > 0 {
		options = append(options, nats.UserCredentials(c.opts.Creds))
	}

	var err error
	c.connection, err = nats.Connect(strings.Join(c.opts.Endpoints, ", "), options...)
	return err
}

func (c *NatsConnection) SetErrorChan(errorChan chan<- error) {
	c.errorChan = errorChan
}

func NewConnection(opts *Options, errorChan chan<- error) (*NatsConnection, error) {
	conn := &NatsConnection{
		Entry: logrus.New().
			WithField("component", "nats").
			WithField("log_tag", opts.LogTag),

		opts:      opts,
		closed:    make(chan error, 1),
		errorChan: errorChan,
	}

	err := conn.connect()

	return conn, err
}

func (c *NatsConnection) Drain() error {
	c.Info("Draining NATS connection...")
	if err := c.connection.Drain(); err != nil {
		c.Errorf("Error when draining NATS connection: %v", err)
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
