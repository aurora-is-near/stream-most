package bridge

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Bridge struct {
	Input         stream.Interface
	Output        stream.Interface
	WriterOptions *block_writer.Options
	ReaderOptions *reader.Options
	Driver        drivers.Driver

	options  *Options
	handlers map[observer.EventType][]func(any)
}

func (b *Bridge) Run(ctx context.Context) error {
	logrus.Debug("Starting the bridge")

	// Determine height on the output stream
	logrus.Debug("Starting stream peek on the output")
	height, err := stream_peek.NewStreamPeek(b.Output).GetTipHeight()
	if err != nil {
		logrus.Error(errors.Wrap(err, "cannot determine the height of the output stream"))
		if errors.Is(err, stream_peek.ErrEmptyStream) {
			height = 0
		} else {
			return err
		}
	}
	logrus.Debug("Stream peek on the output finished, height is ", height)

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

	logrus.Debug("Starting the reader...")
	rdr, err := reader.Start(b.ReaderOptions, b.Input, startSequence, b.options.InputEndSequence)
	if err != nil {
		return errors.Wrap(err, "cannot start the reader")
	}
	defer rdr.Stop()
	logrus.Debug("Reader started")

	// Create a block writer
	logrus.Debug("Starting the writer...")
	writer := block_writer.NewWriter(
		b.WriterOptions.WithDefaults().Validated(),
		b.Output,
		stream_peek.NewStreamPeek(b.Output),
	)
	logrus.Debug("Writer started")

	// Pass messages through the block processor, and write them out
	logrus.Debug("Starting the processor...")
	processor, readerErrors := block_processor.NewProcessorWithReader(
		ctx,
		rdr.Output(),
		b.Driver,
		b.options.ParseTolerance,
	)
	logrus.Debug("Processor started")

	processor.Observer.Register(b.handlers)
	logrus.Debug("Observer registered")

	var writerError error
	writer.OnClose(func(err error) {
		writerError = err
		logrus.Error("Writer closed with error: ", err)
		rdr.Stop()
		processor.Kill()
	})

	logrus.Debug("Starting processor loop...")
	for results := range processor.Run(ctx) {
		if ctx.Err() != nil {
			break
		}

		logrus.Debug("Received results from the processor: ", results.GetBlock().Height)
		err := writer.Write(ctx, results)
		if err != nil {
			logrus.Errorf("Error while writing a message to output stream: %v", err)
			continue
		}
		logrus.Debug("Message written to the output stream")

		processor.Emit(observer.Write, results)
		logrus.Debug("Emitted write event")
	}

	err = <-readerErrors
	if err != nil {
		logrus.Errorf("Reader adapter finished with error: %v", err)
		return errors.Wrap(err, "reader adapter finished with error: ")
	}

	err = b.Driver.FinishError()
	if err != nil {
		logrus.Errorf("Finished with error: %v", err)
		return errors.Wrap(err, "driver finished with error: ")
	}

	if writerError != nil {
		logrus.Errorf("Finishing because of writer's error: %v", writerError)
		return errors.Wrap(writerError, "writer finished with error: ")
	}

	logrus.Info("Finished without error")
	return nil
}

func (b *Bridge) On(write observer.EventType, f func(_ any)) {
	b.handlers[write] = append(b.handlers[write], f)
}

func NewBridge(options *Options, driver drivers.Driver, input, output stream.Interface, writerOptions *block_writer.Options, readerOpts *reader.Options) *Bridge {
	return &Bridge{
		handlers:      map[observer.EventType][]func(any){},
		Driver:        driver,
		Input:         input,
		Output:        output,
		WriterOptions: writerOptions,
		ReaderOptions: readerOpts,
		options:       options,
	}
}
