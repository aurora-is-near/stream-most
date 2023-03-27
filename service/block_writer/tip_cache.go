package block_writer

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"time"
)

type TipCached struct {
	ttl          time.Duration
	lastlyCached time.Time
	tip          messages.AbstractNatsMessage
	peeker       TipPeeker
}

func (t *TipCached) GetTip() (messages.AbstractNatsMessage, error) {
	if t.lastlyCached.Add(t.ttl).Before(time.Now()) {
		return t.getTip()
	}
	return t.tip, nil
}

func (t *TipCached) getTip() (messages.AbstractNatsMessage, error) {
	return t.peeker.GetTip()
}

func NewTipCached(ttl time.Duration, peeker TipPeeker) *TipCached {
	return &TipCached{
		ttl:    ttl,
		peeker: peeker,
	}
}
