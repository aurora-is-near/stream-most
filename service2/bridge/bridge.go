package bridge

import (
	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/aurora-is-near/stream-most/service2/verifier"
)

type Bridge struct {
	verifier verifier.Verifier
	input    blockio.Input
	output   blockio.Output
}
