package nop

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type NopDriver struct {
	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage

	killed bool
}

func (n *NopDriver) BindObserver(obs *observer.Observer) {
	// not using one :)
}

func (n *NopDriver) Kill() {
	n.killed = true
}

func (n *NopDriver) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	n.input = input
	n.output = output
}

func (n *NopDriver) Run() {
	for msg := range n.input {
		if n.killed {
			break
		}
		n.output <- msg
	}
}

func NewNopDriver() *NopDriver {
	return &NopDriver{}
}
