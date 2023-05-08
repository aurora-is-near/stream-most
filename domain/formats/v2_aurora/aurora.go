package v2_aurora

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/nats-io/nats.go"
)

func ParseAuroraBlock(data []byte, _ nats.Header) (*blocks.AbstractBlock, error) {
	block, err := blocks.DecodeAuroraBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}
