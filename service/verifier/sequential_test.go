package verifier

import (
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuroraV2HeadersOnly(t *testing.T) {
	v, err := SequentialForFormat(formats.AuroraV2, true, nil)
	require.NoError(t, err)
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
}

func TestAuroraV2(t *testing.T) {
	v, err := SequentialForFormat(formats.AuroraV2, false, nil)
	require.NoError(t, err)
	run := func(last, next blocks.Block) error {
		return v.CanAppend(
			&messages.BlockMessage{Block: last},
			&messages.BlockMessage{Block: next},
		)
	}

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrHashMismatch,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrHashMismatch,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrHeightGap,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrReannouncement,
	)
}

func TestNearV2HeadersOnly(t *testing.T) {
	v, err := SequentialForFormat(formats.NearV2, true, nil)
	require.NoError(t, err)
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

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
		),
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
}

func TestNearV2(t *testing.T) {
	v, err := SequentialForFormat(formats.NearV2, false, nil)
	require.NoError(t, err)
	run := func(last, next blocks.Block) error {
		return v.CanAppend(
			&messages.BlockMessage{Block: last},
			&messages.BlockMessage{Block: next},
		)
	}

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrHashMismatch,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrHashMismatch,
	)

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrHashMismatch,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrHashMismatch,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrLowHeight,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "notAAA"},
		),
		ErrReannouncement,
	)

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "BBB", PrevHash: "AAA"},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement, Hash: "AAA", PrevHash: "_"},
		),
		ErrReannouncement,
	)
}

func TestNearV3HeadersOnly(t *testing.T) {
	v, err := SequentialForFormat(formats.NearV3, true, nil)
	require.NoError(t, err)
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

	for _, shard1 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 105, 110, 1000, 1001} {
		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
				&blocks.AbstractBlock{Height: 101, ShardID: shard1, BlockType: blocks.Shard},
			),
			ErrUnannouncedBlock,
		)

		assert.NoError(t,
			run(
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
				&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
			),
		)

		for _, shard2 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 105, 110, 1000, 1001} {
			assert.ErrorIs(t,
				run(
					&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
					&blocks.AbstractBlock{Height: 101, ShardID: shard2, BlockType: blocks.Shard},
				),
				ErrUnannouncedBlock,
			)
		}
	}

	assert.NoError(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
		),
	)

	for _, shard1 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
				&blocks.AbstractBlock{Height: 102, ShardID: shard1, BlockType: blocks.Shard},
			),
			ErrUnannouncedBlock,
		)

		assert.NoError(t,
			run(
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
				&blocks.AbstractBlock{Height: 102, BlockType: blocks.Announcement},
			),
		)

		for _, shard2 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
			assert.ErrorIs(t,
				run(
					&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
					&blocks.AbstractBlock{Height: 102, ShardID: shard2, BlockType: blocks.Shard},
				),
				ErrUnannouncedBlock,
			)
		}
	}

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	for _, shard1 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 198, 199, 200, 201, 202, 1000, 1001} {
		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 200, BlockType: blocks.Announcement},
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
			),
			ErrLowHeight,
		)

		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 200, ShardID: shard1, BlockType: blocks.Shard},
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			),
			ErrLowHeight,
		)

		for _, shard2 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 198, 199, 200, 201, 202, 1000, 1001} {
			assert.ErrorIs(t,
				run(
					&blocks.AbstractBlock{Height: 200, ShardID: shard1, BlockType: blocks.Shard},
					&blocks.AbstractBlock{Height: 100, ShardID: shard2, BlockType: blocks.Shard},
				),
				ErrLowHeight,
			)
		}
	}

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrLowHeight,
	)

	for _, shard1 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 101, BlockType: blocks.Announcement},
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
			),
			ErrLowHeight,
		)

		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 101, ShardID: shard1, BlockType: blocks.Shard},
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			),
			ErrLowHeight,
		)

		for _, shard2 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
			assert.ErrorIs(t,
				run(
					&blocks.AbstractBlock{Height: 101, ShardID: shard1, BlockType: blocks.Shard},
					&blocks.AbstractBlock{Height: 100, ShardID: shard2, BlockType: blocks.Shard},
				),
				ErrLowHeight,
			)
		}
	}

	assert.ErrorIs(t,
		run(
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
		),
		ErrReannouncement,
	)

	for _, shard1 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
		assert.NoError(t,
			run(
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
			),
		)

		assert.ErrorIs(t,
			run(
				&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
				&blocks.AbstractBlock{Height: 100, BlockType: blocks.Announcement},
			),
			ErrReannouncement,
		)

		for _, shard2 := range []uint64{0, 1, 2, 98, 99, 100, 101, 102, 103, 105, 110, 1000, 1001} {
			if shard2 <= shard1 {
				assert.ErrorIs(t,
					run(
						&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
						&blocks.AbstractBlock{Height: 100, ShardID: shard2, BlockType: blocks.Shard},
					),
					ErrLowShard,
				)
			} else {
				assert.NoError(t,
					run(
						&blocks.AbstractBlock{Height: 100, ShardID: shard1, BlockType: blocks.Shard},
						&blocks.AbstractBlock{Height: 100, ShardID: shard2, BlockType: blocks.Shard},
					),
				)
			}
		}
	}
}

func TestNearV3HeadersOnlyWithShardFilter(t *testing.T) {
	// TODO
	_ = t
}

func TestNearV3(t *testing.T) {
	// TODO
	_ = t
}

func TestNearV3WithShardFilter(t *testing.T) {
	// TODO
	_ = t
}
