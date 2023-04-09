package main

import (
	"fmt"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/jitter"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
	"os"
)

func run(config Config) error {
	driver := jitter.NewJitter(config.Jitter.Validated())

	input, err := stream.Connect(config.Input)
	if err != nil {
		logrus.Error(err)
		return err
	}

	output, err := stream.Connect(config.Output)
	if err != nil {
		logrus.Error(err)
		return err
	}

	b := bridge.NewBridge(
		config.Bridge,
		driver,
		input,
		output,
		config.Writer,
		config.Reader,
	)
	if err := b.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	return nil
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/jitter/config.json", &config)
	config.Input.Nats.Name = "stream-most-jitter"
	config.Output.Nats.Name = "stream-most-jitter"

	for i := uint64(0); i < config.RestartAttempts; i++ {
		err := run(config)
		if err != nil {
			panic(err)
		}
	}
	os.Exit(1)
}
