package main

import (
	"fmt"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/spf13/viper"
	"os"
)

func run(config Config) {
	driver := near_v3.NewNearV3((&near_v3.Options{
		StuckTolerance:          5,
		StuckRecovery:           true,
		StuckRecoveryWindowSize: 10,
		LastWrittenBlockHash:    nil,
		BlocksCacheSize:         10,
	}).Validated())

	b := bridge.NewBridge(
		driver,
		config.Input,
		config.Output,
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
