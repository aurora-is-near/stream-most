package v2_near

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/nats-io/nats.go"
)

func ParseNearBlock(data []byte, _ nats.Header) (*blocks.AbstractBlock, error) {
	block, err := blocks.DecodeNearBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}
