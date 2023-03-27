package main

import (
	"encoding/json"
	"fmt"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"os"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
)

var config = &bridge.Bridge{
	Input: &stream.Opts{
		Nats: &transport.NatsConnectionConfig{
			Endpoints: []string{
				"",
			},
			Creds:               "nats.creds",
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
		},
		Stream:        "v2_mainnet_near_blocks",
		Subject:       "v2_mainnet_near_blocks",
		RequestWaitMs: 5000,
	},
	Output: &stream.Opts{
		Nats: &transport.NatsConnectionConfig{
			Endpoints: []string{
				"nats://localhost:4222",
			},
			Creds:               "nats.creds",
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "output",
		},
		Stream:        "myblocks",
		Subject:       "myblocks",
		RequestWaitMs: 5000,
	},
	Reader: &stream.ReaderOpts{
		MaxRps:                       2,
		BufferSize:                   1000,
		MaxRequestBatchSize:          250,
		SubscribeAckWaitMs:           5000,
		InactiveThresholdSeconds:     300,
		FetchTimeoutMs:               10000,
		SortBatch:                    true,
		LastSeqUpdateIntervalSeconds: 5,
		Durable:                      "myconsumer",
		StrictStart:                  false,
		WrongSeqToleranceWindow:      50,
	},
	InputStartSequence: 0,
	InputEndSequence:   0,
}

func main() {
	if len(os.Args) < 2 {
		d, _ := json.MarshalIndent(config, "", "  ")
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", string(d))
		os.Exit(1)
	}
	d, err := os.ReadFile(os.Args[1])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error reading config file: %s\n", err)
		os.Exit(1)
	}

	// TODO: please, change it for the love of god
	config = &bridge.Bridge{}
	if err := json.Unmarshal(d, config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s\n", err)
		os.Exit(1)
	}
	config.Input.Nats.Name = "streammost"
	config.Output.Nats.Name = "streammost"
	if err := config.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
