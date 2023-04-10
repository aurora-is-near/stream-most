package bridge

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Bridge struct {
	Input         stream.Interface
	Output        stream.Interface
	WriterOptions *block_writer.Options
	ReaderOptions *reader.Options
	Driver        drivers.Driver

	options *Options
}

func (b *Bridge) Run() error {
	// Determine height on the output stream
	height, err := stream_peek.NewStreamPeek(b.Output).GetTipHeight()
	if err != nil {
		logrus.Error(errors.Wrap(err, "cannot determine the height of the output stream"))
		// TODO: wrap to a custom error type
		if errors.Is(err, nats.ErrMsgNotFound) {
			height = 0
		} else {
			return err
		}
	}

	logrus.Infof("Starting with height of %d", height)

	// Determine the best place to start reading from the input stream
	var startSequence uint64

	logrus.Info("Determining the best sequence to start reading from the input stream...")
	if height == 0 {
		// Output stream is empty, we'll start at the first block announcement found starting from InputStartSequence
		startSequence, err = stream_seek.NewStreamSeek(b.Input).
			SeekFirstAnnouncementBetween(b.options.InputStartSequence, b.options.InputEndSequence)
	} else {
		startSequence, err = stream_seek.NewStreamSeek(b.Input).
			SeekAnnouncementWithHeightBelow(height, b.options.InputStartSequence, b.options.InputEndSequence)
	}
	if err != nil {
		return errors.Wrap(err, "cannot determine the best place to start reading from the input stream")
	}
	logrus.Infof("Starting from the sequence %d", startSequence)

	rdr, err := reader.Start(b.ReaderOptions, b.Input, startSequence, b.options.InputEndSequence)
	if err != nil {
		return errors.Wrap(err, "cannot start the reader")
	}
	defer rdr.Stop()

	// Create a block writer
	writer := block_writer.NewWriter(
		b.WriterOptions.WithDefaults().Validated(),
		b.Output,
		stream_peek.NewStreamPeek(b.Output),
	)

	// Pass messages through the block processor, and write them out
	processor := block_processor.NewProcessorWithReader(
		rdr.Output(),
		b.Driver,
		b.options.ParseTolerance,
	)

	writer.OnClose(func(err error) {
		logrus.Error("Writer closed with error: ", err)
		rdr.Stop()
		processor.Kill()
	})

	for results := range processor.Run() {
		err := writer.Write(context.Background(), results)
		if err != nil {
			logrus.Errorf("Error while writing a message to output stream: %v", err)
		}
	}

	err = b.Driver.FinishError()
	if err != nil {
		logrus.Errorf("Finished with error: %v", err)
		return err
	}

	logrus.Info("Finished without error")
	return nil
}

func NewBridge(options *Options, driver drivers.Driver, input, output stream.Interface, writerOptions *block_writer.Options, readerOpts *reader.Options) *Bridge {
	return &Bridge{
		Driver:        driver,
		Input:         input,
		Output:        output,
		WriterOptions: writerOptions,
		ReaderOptions: readerOpts,
		options:       options,
	}
}
