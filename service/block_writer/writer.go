package block_writer

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Writer struct {
}

func (w *Writer) Write(msg messages.AbstractNatsMessage) error {
	return nil
}

func NewWriter() *Writer {
	return &Writer{}
}
