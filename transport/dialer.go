package transport

import (
	"context"
	"net"
	"sync"
)

type underlyingDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type ctxDialer struct {
	d      underlyingDialer
	ctx    context.Context
	cancel context.CancelCauseFunc

	mtx          sync.Mutex
	attachmentWg sync.WaitGroup
	detach       chan struct{}
}

func newCtxDialer(d underlyingDialer) *ctxDialer {
	cd := &ctxDialer{d: d}
	cd.ctx, cd.cancel = context.WithCancelCause(context.Background())
	return cd
}

func (cd *ctxDialer) Dial(network, address string) (net.Conn, error) {
	return cd.d.DialContext(cd.ctx, network, address)
}

func (cd *ctxDialer) attachContext(ctx context.Context) {
	cd.mtx.Lock()
	defer cd.mtx.Unlock()

	if cd.detach == nil {
		cd.detach = make(chan struct{})
	}

	cd.attachmentWg.Go(func() {
		select {
		case <-ctx.Done():
			cd.cancel(ctx.Err())
		case <-cd.detach:
		}
	})
}

func (cd *ctxDialer) detachAllContexts() {
	cd.mtx.Lock()
	defer cd.mtx.Unlock()

	if cd.detach != nil {
		close(cd.detach)
	}
	cd.attachmentWg.Wait()
	cd.detach = nil
}
