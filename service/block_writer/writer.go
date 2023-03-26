package block_writer

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"time"
)

type Writer struct {
	peek         *stream_peek.StreamPeek // todo: change to interface
	outputStream stream.Interface
}

func (w *Writer) Write(msg messages.AbstractNatsMessage) error {
	_, err := w.outputStream.Write(msg.GetMsg().Data, msg.GetMsg().Header, nats.AckWait(1*time.Second))
	return err
}

func NewWriter(outputStream stream.Interface, peek *stream_peek.StreamPeek) *Writer {
	return &Writer{
		outputStream: outputStream,
		peek:         peek,
	}
}
