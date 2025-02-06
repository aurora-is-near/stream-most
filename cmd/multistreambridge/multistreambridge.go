package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aurora-is-near/stream-most/multistreambridge"
	"github.com/aurora-is-near/stream-most/multistreambridge/inputter"
	"github.com/aurora-is-near/stream-most/multistreambridge/outputter"
)

var configExample = &multistreambridge.Config{
	BlocksFormat: "aurora",
	Output: &outputter.Config{
		NatsEndpoints: []string{
			"clusterX-1.aurora.dev",
			"clusterX-2.aurora.dev",
		},
		NatsCredsPath: "credsX.creds",
		StreamName:    "v100_mainnet_aurora_blocks",
		Subject:       "v100.mainnet.aurora.blocks",
		LogTag:        "x-aurora-out",
		StartHeight:   1337,
	},
	Inputs: []*inputter.Config{
		{
			NatsEndpoints: []string{
				"clusterY-1.aurora.dev",
				"clusterY-2.aurora.dev",
			},
			NatsCredsPath: "credsY.creds",
			StreamName:    "v100_refiner_mainnet_aurora_blocks",
			MinSeq:        42,
			LogTag:        "y-refiner-aurora-in",
			TrustHeaders:  false,
		},
		{
			NatsEndpoints: []string{
				"clusterZ-1.aurora.dev",
				"clusterZ-2.aurora.dev",
			},
			NatsCredsPath: "credsZ.creds",
			StreamName:    "v100_mainnet_aurora_blocks",
			MinSeq:        555,
			LogTag:        "z-refiner-aurora-in",
			TrustHeaders:  true,
		},
	},
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "finished with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		data, err := json.MarshalIndent(configExample, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "%s\n", string(data))
		os.Exit(1)
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		return fmt.Errorf("unable to read config file at '%s': %w", os.Args[1], err)
	}
	cfg := &multistreambridge.Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("can't parse config from JSON file at '%s': %w", os.Args[1], err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	defer cancel()

	return multistreambridge.Run(ctx, cfg)
}
