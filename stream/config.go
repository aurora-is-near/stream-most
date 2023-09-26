package stream

import (
	"time"
)

type Config struct {
	Name        string
	RequestWait time.Duration
	WriteWait   time.Duration
	LogTag      string
}

func (cfg Config) WithDefaults() *Config {
	if cfg.RequestWait == 0 {
		cfg.RequestWait = time.Second * 10
	}
	if cfg.WriteWait == 0 {
		cfg.WriteWait = time.Second * 10
	}
	return &cfg
}
