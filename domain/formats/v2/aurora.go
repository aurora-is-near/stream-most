package v2

import "fmt"

func DecodeAuroraBlock(data []byte) (*Block, error) {
	block, err := DecodeBorealisPayload[Block](data)
	if err != nil {
		return nil, fmt.Errorf("unable to decode aurora v2 block: %w", err)
	}
	return block, nil
}
