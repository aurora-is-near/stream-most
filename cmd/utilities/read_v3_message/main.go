package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/sirupsen/logrus"
)

func chooseConfig(env string) *transport.NATSConfig {
	switch env {
	case "local":
		return &transport.NATSConfig{
			LogTag:  "input",
			Options: transport.RecommendedNatsOptions(),
		}
	case "prod":
		return &transport.NATSConfig{
			OverrideURL:   "tls://developer.nats.backend.aurora.dev:4222/",
			OverrideCreds: "production_developer.creds",
			LogTag:        "input",
			Options:       transport.RecommendedNatsOptions(),
		}
	}

	panic("unknown env")
}

func main() {
	env := "local"

	connectStream, err := stream.Connect(&stream.Options{
		Nats:   chooseConfig(env),
		Stream: "myblocks",
		//Stream:        "v3_mainnet_near_blocks",
		RequestWaitMs: 5000,
	})
	if err != nil {
		panic(err)
	}
	//seq := uint64(3205829527)
	seq := uint64(5900)
	for i := 0; i < 100; i++ {
		seq++

		get, err := connectStream.Get(context.Background(), seq)
		if err != nil {
			panic(err)
		}
		println(get.GetSequence())

		filename := fmt.Sprintf("out/%s/read_v3_message_%d.out", env, seq)
		fileOut, err := os.Create(filename)
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(fileOut, bytes.NewReader(get.GetData()))
		if err != nil {
			panic(err)
		}
		logrus.Infof("Wrote message to %s", filename)
		fileOut.Close()
	}
}
