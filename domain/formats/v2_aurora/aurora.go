package v2_aurora

import (
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

func ParseAuroraBlock(data []byte, header nats.Header) (*types.AbstractBlock, error) {
	block, err := types.DecodeAuroraBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}
