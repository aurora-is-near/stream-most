package block_processor

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_processor/monitoring"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/sirupsen/logrus"
)

// Processor receives messages from the NATS stream,
// processed them using a given driver, monitors them and outputs
type Processor struct {
	*observer.Observer
	input        chan messages.AbstractNatsMessage
	driverOutput chan messages.AbstractNatsMessage
	myOutput     chan messages.AbstractNatsMessage
	driver       drivers.Driver
}

func (g *Processor) work() {
	defer close(g.myOutput)
	g.driver.BindObserver(g.Observer)
	g.driver.Bind(g.input, g.driverOutput)
	g.driver.Run()
}

func (g *Processor) proxyMessages() {
	for msg := range g.driverOutput {
		g.myOutput <- msg

		if msg.IsShard() {
			g.Observer.Emit(observer.NewShard, msg.GetShard())
		}
		if msg.IsAnnouncement() {
			g.Observer.Emit(observer.NewAnnouncement, msg.GetAnnouncement())
		}
	}
}

func (g *Processor) Run() chan messages.AbstractNatsMessage {
	g.driverOutput = make(chan messages.AbstractNatsMessage, 1024)
	g.myOutput = make(chan messages.AbstractNatsMessage, 1024)
	monitoring.RegisterObservations(g.Observer)

	go g.work()
	go g.proxyMessages()
	return g.myOutput
}

func (g *Processor) Kill() {
	g.driver.Kill()
	logrus.Info("Processor is shutting down")
}

func NewProcessor(input chan messages.AbstractNatsMessage, driver drivers.Driver) *Processor {
	return &Processor{
		input:    input,
		driver:   driver,
		Observer: observer.NewObserver(),
	}
}
