package main

import (
	"bytes"
	"fmt"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

func chooseConfig(env string) *transport.Options {
	switch env {
	case "local":
		return &transport.Options{
			Endpoints: []string{
				"nats://localhost:4222/",
			},
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
		}
	case "prod":
		return &transport.Options{
			Endpoints: []string{
				"tls://developer.nats.backend.aurora.dev:4222/",
			},
			Creds:               "production_developer.creds",
			TimeoutMs:           10000,
			PingIntervalMs:      600000,
			MaxPingsOutstanding: 5,
			LogTag:              "input",
		}
	}

	panic("unknown env")
}

func main() {
	env := "prod"

	connectStream, err := stream.Connect(&stream.Options{
		Nats:          chooseConfig(env),
		Stream:        "v3_mainnet_near_blocks",
		RequestWaitMs: 5000,
		//Stream:        "myblocks",
		Subject: "*",
	})
	if err != nil {
		panic(err)
	}
	seq := uint64(3205829527)
	for i := 0; i < 100; i++ {
		seq += 1

		get, err := connectStream.Get(seq)
		if err != nil {
			panic(err)
		}
		println(get.Sequence)

		filename := fmt.Sprintf("out/%s/read_v3_message_%d.out", env, seq)
		fileOut, err := os.Create(filename)
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(fileOut, bytes.NewReader(get.Data))
		if err != nil {
			panic(err)
		}
		logrus.Infof("Wrote message to %s", filename)
		fileOut.Close()
	}
}
