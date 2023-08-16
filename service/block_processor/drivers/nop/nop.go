package nop

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type Driver struct {
	input  chan messages.BlockMessage
	output chan messages.BlockMessage

	killed bool
}

func (n *Driver) BindObserver(_ *observer.Observer) {
	// not using one :)
}

func (n *Driver) Kill() {
	n.killed = true
}

func (n *Driver) Bind(input chan messages.BlockMessage, output chan messages.BlockMessage) {
	n.input = input
	n.output = output
}

func (n *Driver) Run() {
	for msg := range n.input {
		if n.killed {
			break
		}
		n.output <- msg
	}
}

func NewNopDriver() *Driver {
	return &Driver{}
}
