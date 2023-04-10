package adapters

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func ReaderOutputToNatsMessages(input <-chan *reader.Output) chan messages.AbstractNatsMessage {
	in := make(chan messages.AbstractNatsMessage, 1024)
	go func() {
		for k := range input {
			if k.Error != nil {
				logrus.Errorf("Reader adapter: %v", k.Error)
				continue
			}

			message, err := formats.Active().Parse(k.Msg)
			if err != nil {
				logrus.Error(errors.Wrap(err, "failed to decode message: "))
				continue
			}

			in <- message
		}
		close(in)
	}()

	return in
}
