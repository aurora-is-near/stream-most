package main

import (
	"fmt"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/validator"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
	"os"
)

func run(config Config) error {
	input, err := stream.Connect(config.Input)
	if err != nil {
		logrus.Error(err)
		return err
	}

	b := validator.NewValidator(
		input, config.Reader, config.InputStartSequence, config.InputEndSequence,
	)
	if err := b.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	return nil
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/validator/config.json", &config)
	config.Input.Nats.Name = "stream-most-validator"

	go monitor.NewMetricsServer(config.Monitoring).Serve(true)

	formats.UseFormat(config.MessagesFormat)

	for i := uint64(0); i < config.RestartAttempts; i++ {
		err := run(config)
		if err == nil {
			break
		}
		logrus.Errorf("Finished with error: %v", err)
	}
	os.Exit(1)
}
