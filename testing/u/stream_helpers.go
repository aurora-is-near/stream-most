package u

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
)

func DefaultProductionStream() (stream.Interface, error) {
	return stream.Connect(&stream.Options{
		Nats: &transport.Options{
			Endpoints: []string{
				"tls://developer.nats.backend.aurora.dev:4222/",
			},
			Creds:               "../../../production_developer.creds",
			TimeoutMs:           100000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
			Name:                "test-bridge-input",
		},
		Stream:        "v3_mainnet_near_blocks",
		RequestWaitMs: 50000,
	})
}
