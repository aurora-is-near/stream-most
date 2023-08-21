package adapters

import (
	"context"
	"errors"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
)

func ReaderToBlockMessage(ctx context.Context, reader reader.IReader[*messages.BlockMessage], parseTolerance uint) (chan *messages.BlockMessage, chan error) {
	out := make(chan *messages.BlockMessage, 1024)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		remainingParseTolerance := parseTolerance

		for {
			select {
			case <-ctx.Done():
				logrus.Info("Reader adapter: context cancelled, exiting")
				return
			default:
			}

			select {
			case <-ctx.Done():
				logrus.Info("Reader adapter: context cancelled, exiting")
				return
			case msg, ok := <-reader.Output():
				if !ok {
					if reader.Error() != nil {
						errCh <- reader.Error()
						logrus.Infof("Reader adapter: input closed with error: %v, exiting", reader.Error())
					} else {
						logrus.Info("Reader adapter: input closed, exiting")
					}
					return
				}

				select {
				case <-ctx.Done():
					logrus.Info("Reader adapter: context cancelled, exiting")
					return
				case <-msg.WaitDecoding():
				}

				blockMsg, err := msg.Value()
				if err != nil {
					logrus.Warnf("Reader adapter: can't parse message on seq %d, will decrease tolerance: %v", msg.Msg().GetSequence(), blockMsg)
					if remainingParseTolerance == 0 {
						logrus.Error("Reader adapter: too many parse errors in row, exiting")
						errCh <- errors.New("reader adapter: too many parse errors in row")
						return
					}
					remainingParseTolerance--
					continue
				}
				remainingParseTolerance = parseTolerance

				select {
				case <-ctx.Done():
					logrus.Info("Reader adapter: context cancelled, exiting")
					return
				case out <- blockMsg:
				}
			}
		}
	}()

	return out, errCh
}
