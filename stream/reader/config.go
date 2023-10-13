package reader

import (
	"time"
)

type Config struct {
	FilterSubjects []string
	StartSeq       uint64
	EndSeq         uint64
	StrictStart    bool
	MaxSilence     time.Duration
	LogTag         string
}

func (cfg Config) WithDefaults() *Config {
	if len(cfg.FilterSubjects) == 1 && cfg.FilterSubjects[0] == ">" {
		cfg.FilterSubjects = nil
	}
	if cfg.StartSeq == 0 {
		cfg.StartSeq = 1
	}
	if cfg.MaxSilence == 0 {
		cfg.MaxSilence = time.Second * 5
	}
	return &cfg
}

func (cfg Config) WithStartSeq(startSeq uint64) *Config {
	cfg.StartSeq = startSeq
	return &cfg
}

func (cfg Config) WithEndSeq(endSeq uint64) *Config {
	cfg.EndSeq = endSeq
	return &cfg
}
