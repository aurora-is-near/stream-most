package main

import (
	"bytes"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"io"
	"os"
)

func main() {
	config := &transport.NatsConnectionConfig{
		Endpoints: []string{
			"tls://developer.nats.backend.aurora.dev:4222/",
		},
		Creds:               "production_developer.creds",
		TimeoutMs:           10000,
		PingIntervalMs:      600000,
		MaxPingsOutstanding: 5,
		LogTag:              "input",
	}

	connectStream, err := stream.ConnectStream(&stream.Opts{
		Nats:    config,
		Stream:  "v3_mainnet_near_blocks",
		Subject: "*",
	})
	if err != nil {
		panic(err)
	}

	get, err := connectStream.Get(2378379799)
	if err != nil {
		panic(err)
	}

	fileOut, err := os.Create("read_v3_message.out")
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(fileOut, bytes.NewReader(get.Data))
	if err != nil {
		panic(err)
	}
}
