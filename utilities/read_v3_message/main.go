package main

import (
	"bytes"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
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
	seq := uint64(2524980072)
	for i := 0; i < 100; i++ {
		seq += 1

		get, err := connectStream.Get(seq)
		if err != nil {
			panic(err)
		}
		println(get.Sequence)

		fileOut, err := os.Create("out/read_v3_message_" + strconv.Itoa(int(seq)) + ".out")
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(fileOut, bytes.NewReader(get.Data))
		if err != nil {
			panic(err)
		}
		logrus.Info("Wrote message to out/read_v3_message_" + strconv.Itoa(int(seq)) + ".out")
		fileOut.Close()
	}
}
