package block_writer

import (
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type TipCached struct {
	ttl          time.Duration
	lastlyCached time.Time
	tip          messages.BlockMessage
	peeker       TipPeeker
}

func (t *TipCached) GetTip() (messages.BlockMessage, error) {
	if t.lastlyCached.Add(t.ttl).Before(time.Now()) {
		return t.getTip()
	}
	return t.tip, nil
}

func (t *TipCached) getTip() (messages.BlockMessage, error) {
	return t.peeker.GetTip()
}

func NewTipCached(ttl time.Duration, peeker TipPeeker) *TipCached {
	return &TipCached{
		ttl:    ttl,
		peeker: peeker,
	}
}
