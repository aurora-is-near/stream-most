package v2_aurora

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	v2 "github.com/aurora-is-near/stream-most/domain/formats/v2"
	"github.com/nats-io/nats.go"
)

func DecodeAuroraBlock(data []byte, _ nats.Header) (*blocks.AbstractBlock, error) {
	block, err := v2.DecodeBorealisPayload[blocks.AbstractBlock](data)
	if err != nil {
		return nil, fmt.Errorf("unable to decode aurora v2 block: %w", err)
	}
	return block, nil
}
