package main

import (
	"context"
	"os"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/stream_backup"
	"github.com/aurora-is-near/stream-most/stream/autoreader"
	"github.com/aurora-is-near/stream-most/support/when_interrupted"
	"github.com/sirupsen/logrus"
)

func run(config Config) error {
	sb := &stream_backup.StreamBackup{
		Chunks: &chunks.Chunks{
			Dir:                config.OutputDir,
			ChunkNamePrefix:    config.ChunkPrefix + "_",
			CompressionLevel:   config.CompressionLevel,
			MaxEntriesPerChunk: config.MaxChunkEntries,
			MaxChunkSize:       config.MaxChunkSize * 1024 * 1024,
		},
		Reader: autoreader.NewAutoReader(
			config.Input,
			config.Reader,
			formats.DefaultMsgParser,
		),
		StartSeq: config.FromSeq,
		EndSeq:   config.ToSeq,
	}

	return sb.Run()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	when_interrupted.Call(cancel)

	config := Config{}
	configs.ReadTo("cmd/backup/config.json", &config)
	config.Input.Nats.Options.Name = "stream-most"

	formats.UseFormat(config.MessagesFormat)

	go monitor.NewMetricsServer(config.Monitoring).Serve(ctx, true)

	err := run(config)
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	os.Exit(0)
}
