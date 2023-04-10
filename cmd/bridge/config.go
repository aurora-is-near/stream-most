package main

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Bridge *bridge.Options
	Input  *stream.Options
	Output *stream.Options
	Reader *reader.Options
	Writer *block_writer.Options

	ToleranceWindow uint64
	MessagesFormat  formats.FormatType
	Monitoring      *monitor_options.Options
}
