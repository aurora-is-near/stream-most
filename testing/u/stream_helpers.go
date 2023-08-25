package u

import (
	"context"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func DefaultLocalStream() (stream.Interface, error) {
	s, err := stream.Connect(&stream.Options{
		Nats: &transport.NATSConfig{
			LogTag: "input",
			Options: transport.MustApplyNatsOptions(
				transport.RecommendedNatsOptions(),
				nats.Name("test-bridge-input"),
			),
		},
		Stream:        "testing_stream",
		RequestWaitMs: 50000,
	})
	errorMessage := `failed to connect to local stream, please setup localhost:4222 nats server and stream 'testing_stream'.`
	if (s == nil) || (err != nil) {
		logrus.Panic(errorMessage)
	}

	_, err = s.GetInfo(context.Background())
	if err != nil {
		logrus.Panic(errorMessage)
	}
	return s, nil
}

func DefaultProductionStream() (stream.Interface, error) {
	return stream.Connect(&stream.Options{
		Nats: &transport.NATSConfig{
			OverrideURL:   "tls://developer.nats.backend.aurora.dev:4222/",
			OverrideCreds: "../../../production_developer.creds",
			LogTag:        "input",
			Options: transport.MustApplyNatsOptions(
				transport.RecommendedNatsOptions(),
				nats.Name("test-bridge-input"),
			),
		},
		Stream:        "v3_mainnet_near_blocks",
		RequestWaitMs: 50000,
	})
}
