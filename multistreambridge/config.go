package multistreambridge

import (
	"github.com/aurora-is-near/stream-most/multistreambridge/inputter"
	"github.com/aurora-is-near/stream-most/multistreambridge/outputter"
)

type Config struct {
	BlocksFormat string
	Output       *outputter.Config
	Inputs       []*inputter.Config
}
