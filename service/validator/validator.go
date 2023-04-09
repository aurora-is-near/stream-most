package validator

import (
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/validator"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Validator receives blocks from some stream and checks if input stream is in proper format.
// Proper format means that:
// 1. Blocks in the stream are in order of height
// 2. Blocks in the stream contain all their shards
// 3. Shards, if given, are sorted by their ShardID and are following the announcement, not preceding it
// 4. Chain of previous hashes on blocks is not corrupted
type Validator struct {
	Input         stream.Interface
	ReaderOptions *reader.Options

	InputStartSequence uint64
	InputEndSequence   uint64
}

func (b *Validator) Run() error {
	logrus.Info("Determining the best sequence to start reading from the input stream...")
	streamStats, _, err := b.Input.GetInfo(0)
	if err != nil {
		return errors.Wrap(err, "cannot get stream stats: ")
	}

	startingSequence := streamStats.State.FirstSeq
	if b.InputStartSequence > startingSequence {
		startingSequence = b.InputStartSequence
	}

	endingSequence := streamStats.State.LastSeq
	if b.InputEndSequence < endingSequence {
		endingSequence = b.InputEndSequence
	}

	logrus.Infof("Starting from the sequence %d, finishing at %d", startingSequence, endingSequence)
	rdr, err := reader.Start(b.ReaderOptions, b.Input, startingSequence, b.InputEndSequence)
	if err != nil {
		return errors.Wrap(err, "cannot start the reader")
	}
	defer rdr.Stop()

	driver := validator.NewValidator()

	// Pass messages through the block processor
	processor := block_processor.NewProcessorWithReader(rdr.Output(), driver)
	processor.On(observer.ErrorInData, func(data interface{}) {
		d, ok := data.(*observer.WrappedMessage)
		if !ok {
			logrus.Error("ErrorInData: cannot cast data to *observer.WrappedMessage")
		}

		logrus.Errorf("Error in data on sequence %d: %v", d.Message.GetSequence(), d.Wraps)
	})

	<-processor.Run() // Validation driver doesn't write anything and then closes

	err = driver.FinishError()
	if err != nil {
		logrus.Errorf("Finished with error: %v", err)
		return err
	}

	logrus.Info("Finished without error")
	return nil
}

func NewValidator(input stream.Interface, readerOpts *reader.Options, from, to uint64) *Validator {
	return &Validator{
		Input:              input,
		ReaderOptions:      readerOpts,
		InputStartSequence: from,
		InputEndSequence:   to,
	}
}
