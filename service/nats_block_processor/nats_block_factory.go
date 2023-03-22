package nats_block_processor

// Processor receives messages from the NATS stream,
// reorders them, drops out empty ones, and returns them
type Processor struct {
	input  chan ProcessorInput
	output chan ProcessorOutput
}

func (g *Processor) work() {

}

func (g *Processor) Run() <-chan ProcessorOutput {
	g.output = make(chan ProcessorOutput, 1024)
	go g.work()
	return g.output
}

func NewProcessor(input chan ProcessorInput) *Processor {
	return &Processor{
		input: input,
	}
}
