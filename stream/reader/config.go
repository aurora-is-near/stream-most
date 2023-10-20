package reader

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	Consumer    jetstream.OrderedConsumerConfig
	PullOpts    []jetstream.PullConsumeOpt
	EndSeq      uint64
	EndTime     time.Time
	StrictStart bool
	MaxSilence  time.Duration
	LogTag      string
}

func (cfg Config) WithDefaults() *Config {
	if len(cfg.Consumer.FilterSubjects) == 1 && cfg.Consumer.FilterSubjects[0] == ">" {
		cfg.Consumer.FilterSubjects = nil
	}
	if cfg.MaxSilence == 0 {
		cfg.MaxSilence = time.Second * 5
	}
	return &cfg
}
