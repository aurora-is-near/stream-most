package block_writer

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Options struct {
	PublishAckWaitMs           uint
	MaxWriteAttempts           uint
	WriteRetryWaitMs           uint
	TipTtl                     time.Duration
	DisableExpectedCheck       uint64 // Seq of next message
	DisableExpectedCheckHeight uint64 // Height of next message
}

func (o *Options) WithDefaults() *Options {
	if o.PublishAckWaitMs == 0 {
		o.PublishAckWaitMs = 5000
	}
	if o.MaxWriteAttempts == 0 {
		o.MaxWriteAttempts = 3
	}
	if o.TipTtl == 0 {
		o.TipTtl = 15 * time.Second
	}

	return o
}

func (o *Options) Validated() *Options {
	if o.MaxWriteAttempts == 0 {
		panic("MaxWriteAttempts must be set")
	}
	if o.TipTtl < 500*time.Millisecond {
		logrus.Warn("[block_writer] TipTtlSeconds is too low, setting to 0.5s")
		o.TipTtl = 500 * time.Millisecond
	}

	return o
}

func NewOptions() *Options {
	return &Options{}
}
