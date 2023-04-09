package main

import (
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Input              *stream.Options
	Output             *stream.Options
	Reader             *reader.Options
	Writer             *block_writer.Options
	InputStartSequence uint64
	InputEndSequence   uint64

	ToleranceWindow uint64
}
