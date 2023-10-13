package streamconnector

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
)

type Config struct {
	Nats   *transport.NATSConfig
	Stream *stream.Config
}
