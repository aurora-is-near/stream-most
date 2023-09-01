package fake

import (
	"context"
	"sync"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

// Reader is a simple fake for reader.IReader, that only works in pair with fake.Stream
type Reader struct {
	opts     *reader.Options
	input    stream.Interface
	receiver reader.Receiver

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup
}

func StartReader(input stream.Interface, opts *reader.Options, receiver reader.Receiver) (reader.IReader, error) {
	opts = opts.WithDefaults()
	r := &Reader{
		opts:     opts,
		input:    input,
		receiver: receiver,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go r.run()
	return r, nil
}

func (r *Reader) IsFake() bool {
	return true
}

func (r *Reader) Error() error {
	return nil
}

func (r *Reader) Stop(wait bool) {
	r.cancel()
	if wait {
		r.wg.Wait()
	}
}

func (r *Reader) run() {
	r.wg.Done()
	defer r.receiver.HandleFinish(nil)

	for _, item := range r.input.(*Stream).GetArray() {
		if r.ctx.Err() != nil {
			return
		}
		if item.Msg.GetSequence() < r.opts.StartSeq {
			continue
		}
		if r.opts.EndSeq > 0 && item.Msg.GetSequence() >= r.opts.EndSeq {
			break
		}
		r.receiver.HandleMsg(r.ctx, item.Msg)
	}
}
