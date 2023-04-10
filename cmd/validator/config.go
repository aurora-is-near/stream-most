package main

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/jitter"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Input              *stream.Options
	Reader             *reader.Options
	Jitter             *jitter.Options
	Monitoring         *monitor_options.Options
	InputStartSequence uint64
	InputEndSequence   uint64

	RestartAttempts uint64
	MessagesFormat  formats.FormatType
}
