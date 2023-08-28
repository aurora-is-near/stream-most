package tipwatcher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/stream"
)

type TipWatcher struct {
	lastInfo atomic.Pointer[TipInfo]
	ctx      context.Context
	cancel   func()
	wg       sync.WaitGroup
}

func StartTipWatcher(ctx context.Context, input stream.Interface, refreshInterval time.Duration) (*TipWatcher, error) {
	tipInfo := FetchTip(ctx, input)
	if tipInfo.err != nil {
		return nil, fmt.Errorf("can't start tip watcher: unable to fetch initial tip: %w", tipInfo.err)
	}

	w := &TipWatcher{}
	w.lastInfo.Store(tipInfo)
	w.ctx, w.cancel = context.WithCancel(context.Background())

	w.wg.Add(1)
	go w.run(input, refreshInterval)

	return w, nil
}

func (w *TipWatcher) GetTip() *TipInfo {
	return w.lastInfo.Load()
}

func (w *TipWatcher) Stop() {
	w.cancel()
	w.wg.Wait()
}

func (w *TipWatcher) run(input stream.Interface, refreshInterval time.Duration) {
	defer w.wg.Done()

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			prevInfo := w.lastInfo.Load()
			w.lastInfo.Store(FetchTip(w.ctx, input))
			close(prevInfo.outdated)
		}
	}
}
