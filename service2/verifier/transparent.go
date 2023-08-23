package verifier

import "github.com/aurora-is-near/stream-most/domain/messages"

type Transparent struct{}

func (t *Transparent) CanAppend(last, next *messages.BlockMessage) error {
	return nil
}
