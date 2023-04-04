package main

import (
	"fmt"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

func run(config Config) {
	input, err := stream.Connect(config.Input)
	if err != nil {
		logrus.Error(err)
		return
	}

	output, err := stream.Connect(config.Output)
	if err != nil {
		logrus.Error(err)
		return
	}

	var lastWrittenHash *string
	lastWrittenBlock, _, err := stream_seek.NewStreamSeek(output).SeekLastFullyWrittenBlock()
	if err != stream_seek.ErrNotFound {
		lastWrittenHash = &lastWrittenBlock.GetBlock().Hash
	}

	driver := near_v3.NewNearV3((&near_v3.Options{
		StuckTolerance:          5,
		StuckRecovery:           true,
		StuckRecoveryWindowSize: 10,
		LastWrittenBlockHash:    lastWrittenHash,
		BlocksCacheSize:         10,
	}).Validated())

	b := bridge.NewBridge(
		driver,
		input, output,
		config.Reader,
		config.InputStartSequence,
		config.InputEndSequence,
	)
	if err := b.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	}
}

func main() {
	configFile := "cmd/bridge/config.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	viper.SetConfigFile(configFile)
	viper.AddConfigPath(".")
	viper.SetConfigType("json")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	config := Config{}
	if err := viper.Unmarshal(&config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s\n", err)
		os.Exit(1)
	}
	config.Input.Nats.Name = "streammost"
	config.Output.Nats.Name = "streammost"

	for i := uint64(0); i < config.RestartAttempts; i++ {
		run(config)
	}
	os.Exit(1)
}
