package drivers

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type Driver interface {
	FinishError() error
	Bind(input chan *messages.BlockMessage, output chan *messages.BlockMessage)
	BindObserver(observer *observer.Observer)
	Run()
	Kill()
}
