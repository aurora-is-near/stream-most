package verifier

import (
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/stretchr/testify/assert"
)

func TestAuroraHeadersOnly(t *testing.T) {
	v := &Sequential{
		AllowHeightGaps:       false,
		CheckBlocksCompletion: false,
		CheckHashes:           false,
		ShardFilter:           nil,
	}
	run := func(last, next blocks.Block) error {
		return v.CanAppend(
			&messages.BlockMessage{Block: last},
			&messages.BlockMessage{Block: next},
		)
	}

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrReannouncement,
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
		),
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 105, BlockType: blocks.Shard, ShardID: 1},
		),
		ErrUnannouncedBlock,
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
		),
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement},
		),
		ErrHeightGap,
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 1},
		),
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
		),
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 2},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 3},
		),
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 2},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowShard,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowShard,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowShard,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 1},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 99, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 1},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 90, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 1},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Shard, ShardID: 6},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 1},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 0},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 0},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 5},
		),
		ErrUnannouncedBlock,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Shard, ShardID: 5},
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Shard, ShardID: 6},
		),
		ErrUnannouncedBlock,
	)
}

func TestAuroraHeadersOnlyWithShardFilter(t *testing.T) {
	// TODO
	_ = t
}

func TestNearHeadersOnly(t *testing.T) {
	// TODO
	_ = t
}

func TestNearHeadersOnlyWithShardFilter(t *testing.T) {
	// TODO
	_ = t
}

func TestAurora(t *testing.T) {
	// TODO
	_ = t
}

func TestAuroraWithShardFilter(t *testing.T) {
	// TODO
	_ = t
}

func TestNear(t *testing.T) {
	// TODO
	_ = t
}

func TestNearWithShardFilter(t *testing.T) {
	// TODO
	_ = t
}
