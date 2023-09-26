package bridge

import (
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/verifier"
)

type Bridge struct {
	verifier verifier.Verifier
	input    blockio.Input
	output   blockio.Output
}
