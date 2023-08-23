package blocks

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testBlock struct {
	height    uint64
	blockType BlockType
	shardID   uint64
}

func newTestBlock(height uint64, blockType BlockType, shardID uint64) *testBlock {
	return &testBlock{
		height:    height,
		blockType: blockType,
		shardID:   shardID,
	}
}

func (t *testBlock) GetHash() string {
	return ""
}

func (t *testBlock) GetPrevHash() string {
	return ""
}

func (t *testBlock) GetHeight() uint64 {
	return t.height
}

func (t *testBlock) GetBlockType() BlockType {
	return t.blockType
}

func (t *testBlock) GetShardMask() []bool {
	return nil
}

func (t *testBlock) GetShardID() uint64 {
	return t.shardID
}

func (t *testBlock) String() string {
	return fmt.Sprintf("%s:%d.%d", t.blockType.String(), t.height, t.shardID)
}

func TestLess(t *testing.T) {
	blockGroups := [][]*testBlock{
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
