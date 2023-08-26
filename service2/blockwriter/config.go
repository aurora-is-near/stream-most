package blockwriter

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	SubjectPattern                        string
	DisableExpectHeadersCompletely        bool
	DisableExpectHeadersForSeq            uint64
	DisableExpectHeadersForMsgID          string
	DisableExpectHeadersForNextAfterMsgID string
	RetryWait                             time.Duration
	RetryAttempts                         int
	PreserveCustomHeaders                 bool
}

func GetDefaultConfig() *Config {
	return &Config{
		RetryWait:     jetstream.DefaultPubRetryWait,
		RetryAttempts: jetstream.DefaultPubRetryAttempts,
	}
}
