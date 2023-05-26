package adapters

import (
	"context"
	"errors"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
)

func ReaderOutputToNatsMessages(ctx context.Context, input <-chan *reader.Output, parseTolerance uint64) (chan messages.AbstractNatsMessage, chan error) {
	in := make(chan messages.AbstractNatsMessage, 1024)
	errorsOutput := make(chan error, 1)

	parsesFailedInRow := uint64(0)
	go func() {
		defer close(in)
		defer close(errorsOutput)

		for {
			select {
			case <-ctx.Done():
				logrus.Info("Reader adapter: context cancelled, exiting")
				return
			case k, open := <-input:
				if !open {
					return
				}

				if parsesFailedInRow > parseTolerance {
					logrus.Error("Reader adapter: too many parse errors in a row, exiting")
					errorsOutput <- errors.New("too many parse errors in a row")
					break
				}

				if k.Error != nil {
					logrus.Errorf("Reader adapter: %v", k.Error)
					errorsOutput <- k.Error
					break
				}

				if len(k.Msg.Data) == 0 {
					logrus.Warnf("Empty message at sequence %d", k.Metadata.Sequence.Stream)
					continue
				}

				message, err := formats.Active().ParseWithMetadata(k.Msg, k.Metadata)
				if err != nil {
					logrus.Error(err)
					parsesFailedInRow++
					continue
				}

				in <- message
			}
		}
	}()

	return in, errorsOutput
}
