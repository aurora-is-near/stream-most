package stream

import (
	"time"
)

type Config struct {
	Name        string
	RequestWait time.Duration
	WriteWait   time.Duration
	LogTag      string
	AutoCreate  *AutoCreateConfig
}

type AutoCreateConfig struct {
	Subjects    []string
	MsgLimit    int64
	BytesLimit  int64
	Replicas    int
	DedupWindow time.Duration
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
