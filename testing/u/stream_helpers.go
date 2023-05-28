package u

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/sirupsen/logrus"
)

func DefaultLocalStream() (stream.Interface, error) {
	s, err := stream.Connect(&stream.Options{
		Nats: &transport.Options{
			Endpoints: []string{
				"nats://localhost:4222/",
			},
			TimeoutMs:           100000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
			Name:                "test-bridge-input",
		},
		Stream:        "testing_stream",
		RequestWaitMs: 50000,
	})
	errorMessage := `failed to connect to local stream, please setup localhost:4222 nats server and stream 'testing_stream'.`
	if (s == nil) || (err != nil) {
		logrus.Panic(errorMessage)
	}

	_, _, err = s.GetInfo(0)
	if err != nil {
		logrus.Panic(errorMessage)
	}
	return s, nil
}

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
