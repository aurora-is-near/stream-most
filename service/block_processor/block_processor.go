package block_processor

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
)

// Processor receives messages from the NATS stream,
// reorders them, drops out empty ones, and returns them
type Processor struct {
	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage
	driver drivers.Driver
}

func (g *Processor) work() {
	g.driver.Bind(g.input, g.output)
	g.driver.Run()
	close(g.output)
}

func (g *Processor) Run() chan messages.AbstractNatsMessage {
	g.output = make(chan messages.AbstractNatsMessage, 1024)
	go g.work()
	return g.output
}

func NewProcessor(input chan messages.AbstractNatsMessage, driver drivers.Driver) *Processor {
	return &Processor{
		input:  input,
		driver: driver,
	}
}
