package main

import (
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
	"os"
)

// run returns true if any write happened while running
func run(config Config) (bool, error) {
	input, err := stream.Connect(config.Input)
	if err != nil {
		logrus.Error(err)
		return false, err
	}

	output, err := stream.Connect(config.Output)
	if err != nil {
		logrus.Error(err)
		return false, err
	}

	driver := drivers.Infer(drivers.NearV3, input, output)

	writeHappened := false

	b := bridge.NewBridge(
		config.Bridge,
		driver,
		input, output,
		config.Writer,
		config.Reader,
	)
	b.Observer().On(observer.Write, func(_ any) {
		writeHappened = true
	})

	if err := b.Run(); err != nil {
		return writeHappened, err
	}
	return writeHappened, nil
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/bridge/config.json", &config)
	config.Input.Nats.Name = "stream-most"
	config.Output.Nats.Name = "stream-most"

	formats.UseFormat(config.MessagesFormat)

	go monitor.NewMetricsServer(config.Monitoring).Serve(true)

	toleranceAttempts := config.ToleranceWindow

	for toleranceAttempts > 0 {
		writeHappened, err := run(config)
		if err != nil {
			logrus.Error(err)
			if !writeHappened {
				toleranceAttempts -= 1
			}
			continue
		}
		os.Exit(0)
	}
}
