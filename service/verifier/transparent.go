package verifier

import "github.com/aurora-is-near/stream-most/domain/messages"

// Static assertion
var _ Verifier = (*Transparent)(nil)

type Transparent struct{}

func (t *Transparent) WithHeadersOnly() Verifier {
	return t
}

func (t *Transparent) WithNoShardFilter() Verifier {
	return t
}

func (t *Transparent) CanAppend(last, next *messages.BlockMessage) error {
	return nil
}
