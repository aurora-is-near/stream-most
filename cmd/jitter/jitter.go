package main

/*
import (
	"context"
	"fmt"
	"os"

	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/jitter"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support/when_interrupted"
	"github.com/sirupsen/logrus"
)

func run(ctx context.Context, config Config) error {
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
	if err := b.Run(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	when_interrupted.Call(cancel)

	config := Config{}
	configs.ReadTo("cmd/jitter/config.json", &config)
	config.Input.Nats.Options.Name = "stream-most-jitter"
	config.Output.Nats.Options.Name = "stream-most-jitter"

	formats.UseFormat(config.MessagesFormat)

	go monitor.NewMetricsServer(config.Monitoring).Serve(ctx, true)

	for i := uint64(0); i < config.RestartAttempts; i++ {
		err := run(ctx, config)
		if err != nil {
			logrus.Error(err)
		}
	}
	os.Exit(1)
}
*/
