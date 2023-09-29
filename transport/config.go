package transport

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type NATSConfig struct {
	ContextName    string
	OverrideURL    string
	OverrideCreds  string
	LogTag         string
	Options        nats.Options
	OverrideLogger *logrus.Logger
}

func RecommendedNatsOptions() nats.Options {
	opts := nats.GetDefaultOptions()
	opts.MaxReconnect = -1
	opts.Timeout = time.Second * 10
	opts.PingInterval = time.Minute * 10
	opts.MaxPingsOut = 5

	return opts
}

func ApplyNatsOptions(opts nats.Options, applyOpts ...nats.Option) (nats.Options, error) {
	for _, opt := range applyOpts {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return opts, fmt.Errorf("unable to apply NATS option: %w", err)
			}
		}
	}
	return opts, nil
}

func MustApplyNatsOptions(opts nats.Options, applyOpts ...nats.Option) nats.Options {
	opts, err := ApplyNatsOptions(opts, applyOpts...)
	if err != nil {
		panic(err)
	}
	return opts
}
