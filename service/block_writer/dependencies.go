package block_writer

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
)

type TipPeeker interface {
	GetTip() (messages.BlockMessage, error)
}
