package stream

type FakeReader struct {
	opts     *ReaderOpts
	stream   Interface
	startSeq uint64
	endSeq   uint64
	output   chan *ReaderOutput
	closed   bool
}

func StartFakeReader(opts *ReaderOpts, stream Interface, startSeq uint64, endSeq uint64) (IReader, error) {
	return &FakeReader{
		opts:     opts,
		stream:   stream,
		startSeq: startSeq,
		endSeq:   endSeq,
	}, nil
}

func (r *FakeReader) IsFake() bool {
	return true
}

func (r *FakeReader) Output() <-chan *ReaderOutput {
	output := make(chan *ReaderOutput, len(r.stream.(*FakeNearV3Stream).stream))
	r.output = output
	r.run()
	return output
}

func (r *FakeReader) Stop() {
	if !r.closed {
		r.closed = true
	}
}

func (r *FakeReader) run() {
	from := r.startSeq
	to := r.endSeq
	if to == 0 && len(r.stream.(*FakeNearV3Stream).stream) > 0 {
		to = r.stream.(*FakeNearV3Stream).stream[len(r.stream.(*FakeNearV3Stream).stream)-1].GetSequence()
	}

	for _, item := range r.stream.(*FakeNearV3Stream).stream {
		if item.GetSequence() >= from && item.GetSequence() <= to {
			r.output <- &ReaderOutput{
				Msg:      item.GetMsg(),
				Metadata: item.GetMetadata(),
				Error:    nil,
			}
		}
	}
	close(r.output)
}
