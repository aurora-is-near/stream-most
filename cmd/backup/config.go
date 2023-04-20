package main

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/stream"
)

type Config struct {
	Input *stream.Options

	FromSeq uint64
	ToSeq   uint64

	OutputDir        string
	ChunkPrefix      string
	CompressionLevel int

	MaxChunkEntries int
	MaxChunkSize    int // In MB

	MessagesFormat formats.FormatType
	Monitoring     *monitor_options.Options
}
