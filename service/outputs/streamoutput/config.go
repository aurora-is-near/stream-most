package streamoutput

import (
	"time"

	"github.com/aurora-is-near/stream-most/stream/streamconnector"
)

type Config struct {
	Conn                  *streamconnector.Config
	SubjectPattern        string
	WriteRetryWait        time.Duration
	WriteRetryAttempts    int
	PreserveCustomHeaders bool
	MaxReconnects         int
	ReconnectDelay        time.Duration
	StateFetchInterval    time.Duration
}
