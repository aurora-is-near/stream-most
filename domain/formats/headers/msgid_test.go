package headers

import (
	"fmt"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	MsgID          string
	ExpectedResult *MsgIDBlock
	ExpectError    bool
}

func (tc *testCase) run(t *testing.T) {
	result, err := ParseMsgID(tc.MsgID)
	assert.Equal(t, tc.ExpectedResult, result)
	if tc.ExpectError {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

var testCases = []*testCase{
	{
		MsgID: "0",
		ExpectedResult: &MsgIDBlock{
			Height:    0,
			BlockType: blocks.Announcement,
			ShardID:   0,
		},
	},
	{
		MsgID: "1234567890",
		ExpectedResult: &MsgIDBlock{
			Height:    1234567890,
			BlockType: blocks.Announcement,
			ShardID:   0,
		},
	},
	{
		MsgID: "0.0",
		ExpectedResult: &MsgIDBlock{
			Height:    0,
			BlockType: blocks.Shard,
			ShardID:   0,
		},
	},
	{
		MsgID: "0.9876543210",
		ExpectedResult: &MsgIDBlock{
			Height:    0,
			BlockType: blocks.Shard,
			ShardID:   9876543210,
		},
	},
	{
		MsgID: "1234567890.0",
		ExpectedResult: &MsgIDBlock{
			Height:    1234567890,
			BlockType: blocks.Shard,
			ShardID:   0,
		},
	},
	{
		MsgID: "1234567890.9876543210",
		ExpectedResult: &MsgIDBlock{
			Height:    1234567890,
			BlockType: blocks.Shard,
			ShardID:   9876543210,
		},
	},
	{
		MsgID:          "0.",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          ".0",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "1.",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          ".1",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          ".",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "0.0.",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "1.1.",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "0.0.0",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "1.1.1",
		ExpectedResult: nil,
		ExpectError:    true,
	},
	{
		MsgID:          "..",
		ExpectedResult: nil,
		ExpectError:    true,
	},
}

func TestParseMsgID(t *testing.T) {
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d (%s)", i, tc.MsgID), tc.run)
	}
}
