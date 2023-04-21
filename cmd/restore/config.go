package main

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Output *stream.Options

	FromSeq uint64
	ToSeq   uint64

	OutputDir        string
	ChunkPrefix      string
	CompressionLevel int

	MaxChunkEntries int
	MaxChunkSize    int // In MB

	MessagesFormat formats.FormatType
	Monitoring     *monitor_options.Options
	Reader         *reader.Options
	Writer         *block_writer.Options
}
