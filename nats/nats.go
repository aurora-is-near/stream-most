package nats

import (
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	cfg        *Options
	connection *nats.Conn
	closed     chan error
	errorChan  chan<- error
}

func (conn *NatsConnection) SetErrorChan(errorChan chan<- error) {
	conn.errorChan = errorChan
}

func Connect(opts *Options, errorChan chan<- error) (*NatsConnection, error) {
	conn := &NatsConnection{
		cfg:       opts,
		closed:    make(chan error, 1),
		errorChan: errorChan,
	}

	options := []nats.Option{
		nats.Name(opts.Name),
		nats.ReconnectWait(time.Second / 5),
		nats.PingInterval(time.Duration(opts.PingIntervalMs) * time.Millisecond),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.MaxPingsOutstanding(opts.MaxPingsOutstanding),
		nats.Timeout(time.Duration(opts.TimeoutMs) * time.Millisecond),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("NATS [%v] error: %v", opts.LogTag, err)
			if conn.errorChan != nil {
				conn.errorChan <- err
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("NATS [%v] disconnected due to: %v, will try reconnecting", opts.LogTag, err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS [%v] reconnected [%v]", opts.LogTag, nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				log.Printf("NATS [%v] connection closed: %v", opts.LogTag, err)
			}
			conn.closed <- err
		}),
	}
	if len(opts.Creds) > 0 {
		options = append(options, nats.UserCredentials(opts.Creds))
	}

	log.Printf("Connecting to NATS [%v]...", opts.LogTag)
	var err error
	conn.connection, err = nats.Connect(strings.Join(opts.Endpoints, ", "), options...)

	return conn, err
}

func (conn *NatsConnection) Drain() error {
	log.Printf("Draining NATS [%v] connection...", conn.cfg.LogTag)
	if err := conn.connection.Drain(); err != nil {
		log.Printf("Error when draining NATS [%v] connection: %v", conn.cfg.LogTag, err)
		return err
	}
	return nil
}

func (conn *NatsConnection) Conn() *nats.Conn {
	return conn.connection
}

func (conn *NatsConnection) Closed() <-chan error {
	return conn.closed
}
