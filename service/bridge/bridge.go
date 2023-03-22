package bridge

import (
	"github.com/aurora-is-near/stream-bridge/blockwriter"
	"github.com/aurora-is-near/stream-most/service/nats_block_processor"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
)

type Bridge struct {
	Mode               string
	Input              *stream.Opts
	Output             *stream.Opts
	Reader             *stream.ReaderOpts
	Writer             *blockwriter.Opts
	InputStartSequence uint64
	InputEndSequence   uint64
	RestartDelayMs     uint
	ToleranceWindow    uint

	unverified bool
}

func (b *Bridge) Run() error {
	// First, connect to both streams
	inputStream, err := stream.ConnectStream(b.Input)
	if err != nil {
		return err
	}

	outputStream, err := stream.ConnectStream(b.Output)
	if err != nil {
		return err
	}

	// Determine height on the output stream
	height, err := stream_peek.NewStreamPeek(outputStream).PeekTip()
	if err != nil {
		return err
	}

	// Determine the best place to start reading from the input stream
	// TODO: this looks for announcements, but we should also look for their left-most shards
	startSequence, err := stream_seek.NewStreamSeek(inputStream).
		SeekAnnouncementWithHeightBelow(height, b.InputStartSequence, b.InputEndSequence)
	if err != nil {
		return err
	}

	reader, err := stream.StartReader(b.Reader, inputStream, startSequence, b.InputEndSequence)
	if err != nil {
		return err
	}
	defer reader.Stop()

	// Let's make sure we order messages correctly:
	processor := nats_block_processor.NewProcessorWithReader(reader.Output())

	for results := range processor.Run() {

	}
}
