package v2_near

import (
	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

func ParseNearBlock(data []byte, header nats.Header) (*types.AbstractBlock, error) {
	block, err := types.DecodeNearBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}
