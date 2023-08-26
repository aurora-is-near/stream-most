package blocks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestBlock(height uint64, blockType BlockType, shardID uint64) *AbstractBlock {
	return &AbstractBlock{
		Height:    height,
		BlockType: blockType,
		ShardID:   shardID,
	}
}

func TestLess(t *testing.T) {
	blockGroups := [][]*AbstractBlock{
		{
			newTestBlock(100, Announcement, 0),
			newTestBlock(100, Announcement, 0),
			newTestBlock(100, Announcement, 1),
			newTestBlock(100, Announcement, 1),
		},
		{
			newTestBlock(100, Shard, 0),
			newTestBlock(100, Shard, 0),
		},
		{
			newTestBlock(100, Shard, 50),
			newTestBlock(100, Shard, 50),
		},
		{
			newTestBlock(100, Unknown, 0),
			newTestBlock(100, Unknown, 0),
			newTestBlock(100, Unknown, 100),
			newTestBlock(100, Unknown, 100),
		},

		{
			newTestBlock(200, Announcement, 0),
			newTestBlock(200, Announcement, 0),
			newTestBlock(200, Announcement, 1),
			newTestBlock(200, Announcement, 1),
		},
		{
			newTestBlock(200, Shard, 0),
			newTestBlock(200, Shard, 0),
		},
		{
			newTestBlock(200, Shard, 50),
			newTestBlock(200, Shard, 50),
		},
		{
			newTestBlock(200, Unknown, 0),
			newTestBlock(200, Unknown, 0),
			newTestBlock(200, Unknown, 100),
			newTestBlock(200, Unknown, 100),
		},

		{
			newTestBlock(300, Announcement, 0),
			newTestBlock(300, Announcement, 0),
			newTestBlock(300, Announcement, 1),
			newTestBlock(300, Announcement, 1),
		},
		{
			newTestBlock(300, Shard, 0),
			newTestBlock(300, Shard, 0),
		},
		{
			newTestBlock(300, Shard, 50),
			newTestBlock(300, Shard, 50),
		},
		{
			newTestBlock(300, Unknown, 0),
			newTestBlock(300, Unknown, 0),
			newTestBlock(300, Unknown, 100),
			newTestBlock(300, Unknown, 100),
		},
	}

	for aGroupNumber, aGroup := range blockGroups {
		for _, aBlock := range aGroup {
			for bGroupNumber, bGroup := range blockGroups {
				for _, bBlock := range bGroup {
					if aGroupNumber < bGroupNumber {
						assert.Truef(t, Less(aBlock, bBlock), "[%s] must be less than [%s]", aBlock.String(), bBlock.String())
					} else {
						assert.Falsef(t, Less(aBlock, bBlock), "[%s] must not be less than [%s]", aBlock.String(), bBlock.String())
					}
				}
			}
		}
	}
}
