package transport

import (
	"context"
	"net"
	"sync"
)

type ctxConn struct {
	net.Conn
	detachContext func() bool
}

func (cc *ctxConn) Close() error {
	cc.detachContext()
	return cc.Conn.Close()
}

type underlyingDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type ctxDialer struct {
	d      underlyingDialer
	ctx    context.Context
	cancel context.CancelFunc

	mtx    sync.Mutex
	detach []func() bool
}

func newCtxDialer(d underlyingDialer) *ctxDialer {
	cd := &ctxDialer{d: d}
	cd.ctx, cd.cancel = context.WithCancel(context.Background())
	return cd
}

func (cd *ctxDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := cd.d.DialContext(cd.ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &ctxConn{
		Conn: conn,
		detachContext: context.AfterFunc(cd.ctx, func() {
			conn.Close()
		}),
	}, nil
}

func (cd *ctxDialer) attachContext(ctx context.Context) {
	cd.mtx.Lock()
	defer cd.mtx.Unlock()

	cd.detach = append(cd.detach, context.AfterFunc(ctx, cd.cancel))
}

func (cd *ctxDialer) detachAllContexts() bool {
	cd.mtx.Lock()
	defer cd.mtx.Unlock()

	ok := true
	for _, detach := range cd.detach {
		ok = detach() && ok
	}
	cd.detach = nil

	return ok
}
