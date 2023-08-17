package reader

import "github.com/aurora-is-near/stream-most/domain/messages"

type DecodedMsg[T any] struct {
	msg         messages.NatsMessage
	done        chan struct{}
	value       T
	decodingErr error
}

func (d *DecodedMsg[T]) Msg() messages.NatsMessage {
	return d.msg
}

func (d *DecodedMsg[T]) WaitDecoding() <-chan struct{} {
	return d.done
}

func (d *DecodedMsg[T]) Value() (T, error) {
	<-d.done
	return d.value, d.decodingErr
}

func NewPredecodedMsg[T any](original messages.NatsMessage, value T, decodingErr error) *DecodedMsg[T] {
	d := &DecodedMsg[T]{
		msg:         original,
		done:        make(chan struct{}),
		value:       value,
		decodingErr: decodingErr,
	}
	close(d.done)
	return d
}
