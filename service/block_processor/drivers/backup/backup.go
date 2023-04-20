package backup

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/storage"
)

type Backup struct {
	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage

	killed  bool
	obs     *observer.Observer
	storage storage.Interface
}

func (n *Backup) BindObserver(obs *observer.Observer) {
	n.obs = obs
}

func (n *Backup) Kill() {
	n.killed = true
}

func (n *Backup) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	n.input = input
	n.output = output
}

func (n *Backup) Run() {
	for msg := range n.input {
		if n.killed {
			break
		}
		n.output <- msg
	}
}

func (n *Backup) store(message messages.AbstractNatsMessage) {
	n.storage.Write(message)
}

func NewBackup(storage storage.Interface) *Backup {
	return &Backup{
		storage: storage,
	}
}
