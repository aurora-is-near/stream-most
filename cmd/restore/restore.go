package main

import (
	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-most/configs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/service/stream_restore"
	"github.com/sirupsen/logrus"
	"os"
)

func run(config Config) error {
	sb := &stream_restore.StreamRestore{
		Chunks: &chunks.Chunks{
			Dir:                config.OutputDir,
			ChunkNamePrefix:    config.ChunkPrefix + "_",
			CompressionLevel:   config.CompressionLevel,
			MaxEntriesPerChunk: config.MaxChunkEntries,
			MaxChunkSize:       config.MaxChunkSize * 1024 * 1024,
		},
		Stream:          config.Output,
		Writer:          config.Writer,
		StartSeq:        config.FromSeq,
		EndSeq:          config.ToSeq,
		ToleranceWindow: 0,
		ReconnectWaitMs: 0,
	}

	return sb.Run()
}

func main() {
	config := Config{}
	configs.ReadTo("cmd/restore/config.json", &config)
	config.Output.Nats.Name = "stream-most"

	formats.UseFormat(config.MessagesFormat)

	go monitor.NewMetricsServer(config.Monitoring).Serve(true)

	err := run(config)
	if err != nil {
		logrus.Error(err)
		return
	}
	os.Exit(0)
}
