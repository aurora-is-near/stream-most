package block_processor

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/blocks"
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
	input        chan *messages.BlockMessage
	driverOutput chan *messages.BlockMessage
	myOutput     chan *messages.BlockMessage
	driver       drivers.Driver
}

func (g *Processor) work() {
	g.driver.BindObserver(g.Observer)
	g.driver.Bind(g.input, g.driverOutput)
	g.driver.Run()
}

func (g *Processor) proxyMessages(ctx context.Context) {
	defer close(g.myOutput)
loop:
	for {
		select {
		case msg, isOpen := <-g.driverOutput:
			if !isOpen {
				break loop
			}

			g.myOutput <- msg

			switch msg.Block.GetBlockType() {
			case blocks.Shard:
				g.Observer.Emit(observer.NewShard, msg.Block)
			case blocks.Announcement:
				g.Observer.Emit(observer.NewAnnouncement, msg.Block)
			}

		case <-ctx.Done():
			break loop
		}
	}
}

func (g *Processor) Run(ctx context.Context) chan *messages.BlockMessage {
	g.driverOutput = make(chan *messages.BlockMessage, 1024)
	g.myOutput = make(chan *messages.BlockMessage, 1024)

	if monitoring.AnnouncementsProcessed != nil { // TODO: handle normally
		monitoring.RegisterObservations(g.Observer)
	}

	go g.work()
	go g.proxyMessages(ctx)
	return g.myOutput
}

func (g *Processor) Kill() {
	g.driver.Kill()
	logrus.Info("Processor is shutting down")
}

func NewProcessor(input chan *messages.BlockMessage, driver drivers.Driver) *Processor {
	return &Processor{
		input:    input,
		driver:   driver,
		Observer: observer.NewObserver(),
	}
}
