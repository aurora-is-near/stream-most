package verifier

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/formats/headers"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/support/shardmask"
	"github.com/stretchr/testify/require"
)

func TestAuroraV2HeadersOnly(t *testing.T) {
	runTestCases(t, formats.AuroraV2, true, map[error][]string{
		nil: {
			"0 1",
			"1 2",
			"101 102",
		},
		ErrLowHeight: {
			"1 0",
			"5 0",
			"5 4",
			"100 0",
			"1000 999",
		},
		ErrReannouncement: {
			"0 0",
			"1 1",
			"955 955",
		},
		ErrHeightGap: {
			"0 2",
			"1 10",
			"5 1000",
		},
	})
}

func TestAuroraV2(t *testing.T) {
	runTestCases(t, formats.AuroraV2, false, map[error][]string{
		nil: {
			"0/a/b 1/b/c",
			"1/a/a 2/a/a",
			"101/a/a 102/a/b",
		},
		ErrLowHeight: {
			"1/a/b 0/b/c",
			"5/a/a 0/a/a",
			"5/a/a 4/a/b",
			"100/b/a 0/c/d",
			"1000/a/b 999/a/c",
			"1000/a/b 998/b/a",
			"1000/a/b 997/a/a",
		},
		ErrReannouncement: {
			"1/a/b 1/b/c",
			"2/a/a 2/a/a",
			"5/a/a 5/a/b",
			"100/b/a 100/c/d",
			"955/a/b 955/a/c",
			"1000/a/b 1000/b/a",
			"1001/a/b 1001/a/a",
		},
		ErrHeightGap: {
			"0/a/b 2/b/c",
			"0/a/a 5/a/a",
			"1/a/a 100/a/b",
			"5/b/a 100/c/d",
			"100/a/b 1000/a/c",
			"998/a/b 1000/b/a",
			"998/a/b 1001/a/a",
		},
		ErrHashMismatch: {
			"0/a/b 1/c/d",
			"1/b/a 2/c/d",
			"4/a/b 5/a/c",
			"999/a/b 1000/a/a",
			"1000/a/b 1001/a/b",
		},
	})
}

func TestNearV2HeadersOnlyX(t *testing.T) {
	runTestCases(t, formats.NearV2, true, map[error][]string{
		nil: {
			"0 1",
			"0 2",
			"1 2",
			"1 10",
			"5 1000",
			"101 102",
		},
		ErrLowHeight: {
			"1 0",
			"5 0",
			"5 4",
			"100 0",
			"1000 999",
		},
		ErrReannouncement: {
			"0 0",
			"1 1",
			"955 955",
		},
	})
}

func TestNearV2(t *testing.T) {
	runTestCases(t, formats.NearV2, false, map[error][]string{
		nil: {
			"0/a/b 1/b/c",
			"0/a/b 2/b/c",
			"0/a/a 5/a/a",
			"1/a/a 2/a/a",
			"1/a/a 100/a/b",
			"101/a/a 102/a/b",
			"998/a/b 1000/b/a",
		},
		ErrLowHeight: {
			"1/a/b 0/b/c",
			"5/a/a 0/a/a",
			"5/a/a 4/a/b",
			"100/b/a 0/c/d",
			"1000/a/b 999/a/c",
			"1000/a/b 998/b/a",
			"1000/a/b 997/a/a",
		},
		ErrReannouncement: {
			"1/a/b 1/b/c",
			"2/a/a 2/a/a",
			"5/a/a 5/a/b",
			"100/b/a 100/c/d",
			"955/a/b 955/a/c",
			"1000/a/b 1000/b/a",
			"1001/a/b 1001/a/a",
		},
		ErrHashMismatch: {
			"0/a/b 1/c/d",
			"1/b/a 2/c/d",
			"4/a/b 5/a/c",
			"5/b/a 100/c/d",
			"100/a/b 1000/a/c",
			"998/a/b 1001/a/a",
			"999/a/b 1000/a/a",
			"1000/a/b 1001/a/b",
		},
	})
}

func TestNearV3HeadersOnly(t *testing.T) {
	runTestCases(t, formats.NearV3, true, map[error][]string{
		nil: {
			// TODO
		},
		ErrFilteredShard: {
			// TODO
		},
		ErrLowHeight: {
			// TODO
		},
		ErrReannouncement: {
			// TODO
		},
		ErrLowShard: {
			// TODO
		},
		ErrUnannouncedBlock: {
			// TODO
		},
	})
}

func TestNearV3(t *testing.T) {
	runTestCases(t, formats.NearV3, true, map[error][]string{
		nil: {
			// TODO
		},
		ErrFilteredShard: {
			// TODO
		},
		ErrLowHeight: {
			// TODO
		},
		ErrReannouncement: {
			// TODO
		},
		ErrLowShard: {
			// TODO
		},
		ErrUnwantedShard: {
			// TODO
		},
		ErrShardHashConflict: {
			// TODO
		},
		ErrIncompletePreviousBlock: {
			// TODO
		},
		ErrUnannouncedBlock: {
			// TODO
		},
		ErrHashMismatch: {
			// TODO
		},
		ErrShardGap: {
			// TODO
		},
	})
}

func runTestCases(t *testing.T, format formats.FormatType, headersOnly bool, testCases map[error][]string) {
	for expectedErr, testCasesForError := range testCases {
		for _, testCaseDesc := range testCasesForError {
			expectedErr, testCaseDesc := expectedErr, testCaseDesc

			t.Run(testCaseDesc, func(t *testing.T) {
				tc := parseTestCase(testCaseDesc)
				v, err := SequentialForFormat(format, headersOnly, tc.filter)
				require.NoError(t, err)

				resultErr := v.CanAppend(
					&messages.BlockMessage{Block: tc.last},
					&messages.BlockMessage{Block: tc.next},
				)

				if expectedErr == nil {
					require.NoError(t, resultErr)
				} else {
					require.ErrorIs(t, resultErr, expectedErr)
				}
			})
		}
	}
}

type testCase struct {
	desc       string
	last, next *blocks.AbstractBlock
	filter     []bool
}

func parseTestCase(s string) *testCase {
	tc := &testCase{desc: s}

	tokens := strings.Split(s, " ")
	if len(tokens) != 2 && len(tokens) != 3 {
		panic(len(tokens))
	}

	tc.last, tc.next = parseTestBlock(tokens[0]), parseTestBlock(tokens[1])
	if len(tokens) == 2 {
		return tc
	}

	tc.filter = parseShardMask(tokens[2])
	return tc
}

func parseTestBlock(s string) *blocks.AbstractBlock {
	b := &blocks.AbstractBlock{}

	tokens := strings.Split(s, "/")
	if len(tokens) < 1 {
		panic(len(tokens))
	}

	msgidBlock, err := headers.ParseMsgID(tokens[0])
	if err != nil {
		panic(err)
	}
	b.Height = msgidBlock.Height
	b.BlockType = msgidBlock.BlockType
	b.ShardID = msgidBlock.ShardID

	if len(tokens) == 1 {
		return b
	}

	if len(tokens) < 3 {
		panic(len(tokens))
	}
	b.PrevHash = tokens[1]
	b.Hash = tokens[2]

	if len(tokens) == 3 {
		return b
	}

	if len(tokens) > 4 {
		panic(len(tokens))
	}

	b.ShardMask = parseShardMask(tokens[3])

	return b
}

func parseShardMask(s string) []bool {
	tokens := strings.Split(s, "+")

	if len(tokens) < 1 {
		panic(len(tokens))
	}
	mask, err := shardmask.ParseShardMask(tokens[0])
	if err != nil {
		panic(err)
	}
	if len(tokens) == 1 {
		return mask
	}

	if len(tokens) > 2 {
		panic(len(tokens))
	}
	addLen, err := strconv.ParseUint(tokens[1], 10, 64)
	if err != nil {
		panic(err)
	}
	resizedMask := make([]bool, uint64(len(mask))+addLen)
	copy(resizedMask, mask)
	return resizedMask
}

func TestParseTestCase(t *testing.T) {
	testCases := []*testCase{
		{
			desc: "10 10.5",
			last: &blocks.AbstractBlock{
				Height:    10,
				BlockType: blocks.Announcement,
				ShardID:   0,
				PrevHash:  "",
				Hash:      "",
				ShardMask: nil,
			},
			next: &blocks.AbstractBlock{
				Height:    10,
				BlockType: blocks.Shard,
				ShardID:   5,
				PrevHash:  "",
				Hash:      "",
				ShardMask: nil,
			},
			filter: nil,
		},
		{
			desc: "10.5/A/B 0.0/AcD/BcD/1,2,3 1-2+5",
			last: &blocks.AbstractBlock{
				Height:    10,
				BlockType: blocks.Shard,
				ShardID:   5,
				PrevHash:  "A",
				Hash:      "B",
				ShardMask: nil,
			},
			next: &blocks.AbstractBlock{
				Height:    0,
				BlockType: blocks.Shard,
				ShardID:   0,
				PrevHash:  "AcD",
				Hash:      "BcD",
				ShardMask: []bool{false, true, true, true},
			},
			filter: []bool{false, true, true, false, false, false, false, false},
		},
		{
			desc: "10.5/A/B/ 0.0/A/B/- +0",
			last: &blocks.AbstractBlock{
				Height:    10,
				BlockType: blocks.Shard,
				ShardID:   5,
				PrevHash:  "A",
				Hash:      "B",
				ShardMask: nil,
			},
			next: &blocks.AbstractBlock{
				Height:    0,
				BlockType: blocks.Shard,
				ShardID:   0,
				PrevHash:  "A",
				Hash:      "B",
				ShardMask: []bool{},
			},
			filter: []bool{},
		},
		{
			desc: "10.5/A/B/0 0.0/A/B/5 2,1+1",
			last: &blocks.AbstractBlock{
				Height:    10,
				BlockType: blocks.Shard,
				ShardID:   5,
				PrevHash:  "A",
				Hash:      "B",
				ShardMask: []bool{true},
			},
			next: &blocks.AbstractBlock{
				Height:    0,
				BlockType: blocks.Shard,
				ShardID:   0,
				PrevHash:  "A",
				Hash:      "B",
				ShardMask: []bool{false, false, false, false, false, true},
			},
			filter: []bool{false, true, true, false},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc, parseTestCase(tc.desc))
		})
	}
}
