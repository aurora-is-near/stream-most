package drivers

import "github.com/aurora-is-near/stream-most/domain/messages"

type Driver interface {
	Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage)
	Run()
}
