package streaminput

import (
	"time"

	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	Conn               *streamconnector.Config
	FilterSubjects     []string
	MaxSilence         time.Duration
	BufferSize         uint
	MaxReconnects      int
	ReconnectDelay     time.Duration
	StateFetchInterval time.Duration
	LogInterval        time.Duration
	PullOpts           []jetstream.PullConsumeOpt
}
