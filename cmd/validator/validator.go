package main

import (
	"context"
	"fmt"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/validator"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support/when_interrupted"
	"github.com/sirupsen/logrus"
	"os"
)

func run(ctx context.Context, config Config) error {
	input, err := stream.Connect(config.Input)
	if err != nil {
		logrus.Error(err)
		return err
	}

	b := validator.NewValidator(
		input, config.Reader, config.InputStartSequence, config.InputEndSequence,
	)
	if err := b.Run(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	when_interrupted.Call(cancel)

	config := Config{}
	configs.ReadTo("cmd/validator/config.json", &config)
	config.Input.Nats.Name = "stream-most-validator"

	go monitor.NewMetricsServer(config.Monitoring).Serve(ctx, true)

	formats.UseFormat(config.MessagesFormat)

	for i := uint64(0); i < config.RestartAttempts; i++ {
		err := run(ctx, config)
		if err == nil {
			break
		}
		logrus.Errorf("Finished with error: %v", err)
	}
	os.Exit(1)
}
