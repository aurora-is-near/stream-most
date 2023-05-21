package block_writer

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Options struct {
	// All validations (height & hash) are disabled if this is set to true
	BypassValidation bool

	// How much "low height" errors in row should we receive
	// so that we decide that we are outdated and need to restart
	// This one considers duplicates (as indicated by NATS) as "low height" as well
	LowHeightTolerance uint64

	PublishAckWaitMs           uint
	MaxWriteAttempts           uint
	WriteRetryWaitMs           uint
	TipTtl                     time.Duration
	DisableExpectedCheck       uint64 // Seq of next message
	DisableExpectedCheckHeight uint64 // Height of next message
}

func (o Options) WithDefaults() *Options {
	if o.LowHeightTolerance == 0 {
		o.LowHeightTolerance = 20
	}
	if o.PublishAckWaitMs == 0 {
		o.PublishAckWaitMs = 5000
	}
	if o.MaxWriteAttempts == 0 {
		o.MaxWriteAttempts = 3
	}
	if o.TipTtl == 0 {
		o.TipTtl = 15 * time.Second
	}

	return &o
}

func (o Options) Validated() *Options {
	if o.MaxWriteAttempts == 0 {
		panic("MaxWriteAttempts must be set")
	}
	if o.TipTtl < 500*time.Millisecond {
		logrus.Warn("[block_writer] TipTtlSeconds is too low, setting to 0.5s")
		o.TipTtl = 500 * time.Millisecond
	}

	if o.BypassValidation {
		logrus.Warn("[block_writer] BypassValidation is set to true, all validations are disabled!")
	}

	return &o
}

func NewOptions() *Options {
	return &Options{}
}
