package bridge

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

type Bridge struct {
	Input  *stream.Opts
	Output *stream.Opts
	Reader *stream.ReaderOpts

	InputStartSequence uint64
	InputEndSequence   uint64
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
	height, err := stream_peek.NewStreamPeek(outputStream).GetTipHeight()
	if err != nil {
		return err
	}

	// Determine the best place to start reading from the input stream
	var startSequence uint64

	if height == 0 {
		// Output stream is empty, we'll start at the first block announcement found starting from InputStartSequence
		startSequence, err = stream_seek.NewStreamSeek(inputStream).
			SeekFirstAnnouncementBetween(b.InputStartSequence, b.InputEndSequence)
	} else {
		startSequence, err = stream_seek.NewStreamSeek(inputStream).
			SeekAnnouncementWithHeightBelow(height, b.InputStartSequence, b.InputEndSequence)
	}
	if err != nil {
		return err
	}

	reader, err := stream.StartReader(b.Reader, inputStream, startSequence, b.InputEndSequence)
	if err != nil {
		return err
	}
	defer reader.Stop()

	// Create a block writer
	writer := block_writer.NewWriter(
		block_writer.NewOptions().WithDefaults().Validated(),
		outputStream,
		stream_peek.NewStreamPeek(outputStream),
	)

	// Pass messages through the block processor, and write them out
	driver := near_v3.NewNearV3NoSorting(
		stream_seek.NewStreamSeek(inputStream),
	)

	processor := block_processor.NewProcessorWithReader(reader.Output(), driver)

	for results := range processor.Run() {
		err := writer.Write(context.Background(), results)
		if err != nil {
			logrus.Error(err)
		}
	}

	logrus.Info("Finished!")
	return nil
}

func NewBridge(inputOpts, outputOpts *stream.Opts, readerOpts *stream.ReaderOpts, from, to uint64) *Bridge {
	return &Bridge{
		Input:              inputOpts,
		Output:             outputOpts,
		Reader:             readerOpts,
		InputStartSequence: from,
		InputEndSequence:   to,
	}
}
