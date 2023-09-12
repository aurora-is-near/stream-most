package streamoutput

import (
	"time"

	"github.com/aurora-is-near/stream-most/stream"
)

type Config struct {
	Stream                *stream.Options
	SubjectPattern        string
	WriteRetryWait        time.Duration
	WriteRetryAttempts    int
	PreserveCustomHeaders bool
	MaxReconnects         int
	ReconnectDelay        time.Duration
	StateFetchInterval    time.Duration
}
