package bridge

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/jitter"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Bridge struct {
	Input  *stream.Options
	Output *stream.Options
	Reader *reader.Options
	Jitter *jitter.Options

	InputStartSequence uint64
	InputEndSequence   uint64
	Driver             drivers.Driver
}

func (b *Bridge) Run() error {
	// First, connect to both streams
	inputStream, err := stream.Connect(b.Input)
	if err != nil {
		return errors.Wrap(err, "cannot connect the input stream")
	}

	outputStream, err := stream.Connect(b.Output)
	if err != nil {
		return errors.Wrap(err, "cannot connect the output stream")
	}

	// Determine height on the output stream
	height, err := stream_peek.NewStreamPeek(outputStream).GetTipHeight()
	if err != nil {
		return errors.Wrap(err, "cannot determine the height of the output stream")
	}

	logrus.Infof("Starting with height of %d", height)

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
		return errors.Wrap(err, "cannot determine the best place to start reading from the input stream")
	}

	rdr, err := reader.Start(b.Reader, inputStream, startSequence, b.InputEndSequence)
	if err != nil {
		return errors.Wrap(err, "cannot start the reader")
	}
	defer rdr.Stop()

	// Create a block writer
	writer := block_writer.NewWriter(
		block_writer.NewOptions().WithDefaults().Validated(),
		outputStream,
		stream_peek.NewStreamPeek(outputStream),
	)

	// Pass messages through the block processor, and write them out
	processor := block_processor.NewProcessorWithReader(rdr.Output(), b.Driver)

	for results := range processor.Run() {
		err := writer.Write(context.Background(), results)
		if err != nil {
			logrus.Error(err)
		}
	}

	err = b.Driver.Error()
	if err != nil {
		logrus.Errorf("Finished with error: %v", err)
		return err
	}

	logrus.Info("Finished!")
	return nil
}

func NewBridge(driver drivers.Driver, inputOpts, outputOpts *stream.Options, readerOpts *reader.Options, from, to uint64) *Bridge {
	return &Bridge{
		Driver:             driver,
		Input:              inputOpts,
		Output:             outputOpts,
		Reader:             readerOpts,
		InputStartSequence: from,
		InputEndSequence:   to,
	}
}
