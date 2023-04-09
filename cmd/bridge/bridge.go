package main

import (
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
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

	output, err := stream.Connect(config.Output)
	if err != nil {
		logrus.Error(err)
		return err
	}

	driver := drivers.Infer(drivers.NearV3, input, output)

	b := bridge.NewBridge(
		config.Bridge,
		driver,
		input, output,
		config.Writer,
		config.Reader,
	)
	if err := b.Run(); err != nil {
		return err
	}
	return nil
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/bridge/config.json", &config)
	config.Input.Nats.Name = "stream-most"
	config.Output.Nats.Name = "stream-most"

	go monitor.NewMetricsServer().Serve(true)

	for i := uint64(0); i < config.ToleranceWindow; i++ {
		err := run(config)
		if err != nil {
			logrus.Error(err)
			continue
		}
	}
	os.Exit(0)
}
