package transport

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type NATSConfig struct {
	ContextName    string
	ServerURL      string
	Creds          string
	LogTag         string
	Options        *nats.Options
	OverrideLogger *logrus.Logger
}

func RecommendedNatsOptions(applyOpts ...nats.Option) *nats.Options {
	opts := nats.GetDefaultOptions()
	opts.AllowReconnect = true
	opts.MaxReconnect = -1
	opts.Timeout = time.Second * 10
	opts.PingInterval = time.Minute * 10
	opts.MaxPingsOut = 5

	return MustApplyNatsOptions(&opts, applyOpts...)
}

func ApplyNatsOptions(opts *nats.Options, applyOpts ...nats.Option) error {
	for _, opt := range applyOpts {
		if opt != nil {
			if err := opt(opts); err != nil {
				return fmt.Errorf("unable to apply NATS option: %w", err)
			}
		}
	}
	return nil
}

func MustApplyNatsOptions(opts *nats.Options, applyOpts ...nats.Option) *nats.Options {
	if err := ApplyNatsOptions(opts, applyOpts...); err != nil {
		panic(err)
	}
	return opts
}
