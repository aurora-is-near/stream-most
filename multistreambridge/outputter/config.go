package outputter

import (
	"github.com/aurora-is-near/stream-most/stream"
)

type Config struct {
	NatsEndpoints []string
	NatsCredsPath string
	StreamName    string
	Subject       string
	LogTag        string
	StartHeight   uint64
	AutoCreate    *stream.AutoCreateConfig
}
