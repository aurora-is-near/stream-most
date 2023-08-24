package headers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type MsgIDBlock struct {
	Height    uint64
	BlockType blocks.BlockType
	ShardID   uint64
}

func (b *MsgIDBlock) GetHash() string {
	return ""
}

func (b *MsgIDBlock) GetPrevHash() string {
	return ""
}

func (b *MsgIDBlock) GetHeight() uint64 {
	return b.Height
}

func (b *MsgIDBlock) GetBlockType() blocks.BlockType {
	return b.BlockType
}

func (b *MsgIDBlock) GetShardMask() []bool {
	return nil
}

func (b *MsgIDBlock) GetShardID() uint64 {
	return b.ShardID
}

func ParseMsgID(msgID string) (*MsgIDBlock, error) {
	heightStr, shardStr, hasDot := strings.Cut(msgID, ".")

	height, err := strconv.ParseUint(heightStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse block height: %w", err)
	}

	if !hasDot {
		return &MsgIDBlock{
			Height:    height,
			BlockType: blocks.Announcement,
		}, nil
	}

	shard, err := strconv.ParseUint(shardStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse block height: %w", err)
	}

	return &MsgIDBlock{
		Height:    height,
		BlockType: blocks.Shard,
		ShardID:   shard,
	}, nil
}
