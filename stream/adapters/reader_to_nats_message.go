package adapters

import (
	"errors"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
)

func ReaderOutputToNatsMessages(input <-chan *reader.Output, parseTolerance uint64) (chan messages.AbstractNatsMessage, chan error) {
	in := make(chan messages.AbstractNatsMessage, 1024)
	errorsOutput := make(chan error, 1)

	parsesFailedInRow := uint64(0)
	go func() {
		for k := range input {
			if parsesFailedInRow > parseTolerance {
				logrus.Error("Reader adapter: too many parse errors in a row, exiting")
				errorsOutput <- errors.New("too many parse errors in a row")
				break
			}

			if k.Error != nil {
				logrus.Errorf("Reader adapter: %v", k.Error)
				continue
			}

			if len(k.Msg.Data) == 0 {
				logrus.Warnf("Empty message at sequence %d", k.Metadata.Sequence.Stream)
				continue
			}

			message, err := formats.Active().ParseWithMetadata(k.Msg, k.Metadata)
			if err != nil {
				logrus.Error(err)
				parsesFailedInRow += 1
				continue
			}

			in <- message
		}
		close(in)
		close(errorsOutput)
	}()

	return in, errorsOutput
}
