package streaminput

import (
	"time"

	"github.com/aurora-is-near/stream-most/stream/streamconnector"
)

type Config struct {
	Conn               *streamconnector.Config
	FilterSubjects     []string
	MaxSilence         time.Duration
	BufferSize         uint
	MaxReconnects      int
	ReconnectDelay     time.Duration
	StateFetchInterval time.Duration
}
