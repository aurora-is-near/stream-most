package drivers

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
)

type Driver interface {
	Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage)
	BindObserver(observer *observer.Observer)
	Run()
}
