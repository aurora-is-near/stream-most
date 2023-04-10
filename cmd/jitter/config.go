package main

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/jitter"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Bridge             *bridge.Options
	Input              *stream.Options
	Output             *stream.Options
	Writer             *block_writer.Options
	Reader             *reader.Options
	Jitter             *jitter.Options
	InputStartSequence uint64
	InputEndSequence   uint64

	RestartAttempts uint64
	MessagesFormat  formats.FormatType
}
