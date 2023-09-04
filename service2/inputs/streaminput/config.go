package streaminput

import (
	"time"

	"github.com/aurora-is-near/stream-most/stream"
)

type Config struct {
	Stream             *stream.Options
	FilterSubjects     []string
	StartSeq           uint64
	EndSeq             uint64
	MaxSilence         time.Duration
	BufferSize         uint
	MaxReconnects      int
	ReconnectDelay     time.Duration
	StateFetchInterval time.Duration
}
