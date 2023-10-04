package bridge

import (
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/verifier"
)

type Bridge struct {
	config   *Config
	verifier verifier.Verifier
	input    blockio.Input
	output   blockio.Output
}

func Start(cfg *Config, verifier verifier.Verifier, input blockio.Input, output blockio.Output) (*Bridge, error) {
	return nil, nil
}
