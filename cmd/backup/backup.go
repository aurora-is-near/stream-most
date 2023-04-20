package main

import (
	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/stream_backup"
	"github.com/aurora-is-near/stream-most/stream/autoreader"
	"github.com/sirupsen/logrus"
	"os"
)

func run(config Config) error {
	sb := &stream_backup.StreamBackup{
		Mode: "",
		Chunks: &chunks.Chunks{
			Dir:                config.OutputDir,
			ChunkNamePrefix:    config.ChunkPrefix + "_",
			CompressionLevel:   config.CompressionLevel,
			MaxEntriesPerChunk: config.MaxChunkEntries,
			MaxChunkSize:       config.MaxChunkSize * 1024 * 1024,
		},
		Reader: autoreader.NewAutoReader(
			config.FromSeq,
			config.ToSeq,
			config.Input,
		),
	}

	return sb.Run()
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/backup/config.json", &config)
	config.Input.Nats.Name = "stream-most"

	formats.UseFormat(config.MessagesFormat)

	go monitor.NewMetricsServer(config.Monitoring).Serve(true)

	err := run(config)
	if err != nil {
		logrus.Error(err)
		return
	}
	os.Exit(0)
}
