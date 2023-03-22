package bridge

import (
	"github.com/aurora-is-near/stream-bridge/blockwriter"
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
	startSequence, err := stream_seek.NewStreamSeek(inputStream).
		SeekAnnouncementWithHeightBelow(height, b.InputStartSequence, b.InputEndSequence)
	if err != nil {
		return err
	}

	// Let's work!

}
