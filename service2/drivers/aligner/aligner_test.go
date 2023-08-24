package aligner

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service2/drivers"
	"github.com/aurora-is-near/stream-most/service2/verifier"
	"github.com/stretchr/testify/assert"
)

type simpleVerifier struct{}

func (v *simpleVerifier) CanAppend(last, next *messages.BlockMessage) error {
	if last == nil || next.Block.GetHeight() == last.Block.GetHeight()+1 {
		return nil
	}
	switch {
	case next.Block.GetHeight() < last.Block.GetHeight():
		return verifier.ErrLowHeight
	case next.Block.GetHeight() > last.Block.GetHeight():
		return verifier.ErrHeightGap
	default:
		return verifier.ErrReannouncement
	}
}

func quickBlock(height uint64) *messages.BlockMessage {
	return &messages.BlockMessage{
		Block: &blocks.AbstractBlock{
			Height:    height,
			BlockType: blocks.Announcement,
		},
	}
}

type testCase struct {
	Config *Config
	Input  []uint64
	Output []uint64
	Err    error
}

func (tc *testCase) run(t *testing.T) {
	a := NewAligner(tc.Config, &simpleVerifier{})

	out := []uint64{}
	var resultErr error

	for _, in := range tc.Input {

		pendingMsg := quickBlock(in)
		for {
			var tip *messages.BlockMessage
			if len(out) > 0 {
				tip = quickBlock(out[len(out)-1])
			}

			nxt, err := a.Next(tip, pendingMsg, nil)
			if err != nil {
				if errors.Is(err, drivers.ErrNoNext) {
					break
				} else {
					resultErr = err
					break
				}
			}
			if nxt == nil {
				break
			}
			out = append(out, nxt.Block.GetHeight())
			pendingMsg = nil
		}

		if resultErr != nil {
			break
		}
	}

	assert.Equal(t, tc.Output, out)
	if tc.Err == nil {
		assert.NoError(t, resultErr)
	} else {
		assert.ErrorIs(t, resultErr, tc.Err)
	}
}

var testCases = []*testCase{
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   0,
			WrongBlocksTolerance: 0,
			NoWriteTolerance:     0,
		},
		Input:  []uint64{1, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   5,
			WrongBlocksTolerance: 5,
			NoWriteTolerance:     5,
		},
		Input:  []uint64{1, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    10,
			LowBlocksTolerance:   5,
			WrongBlocksTolerance: 5,
			NoWriteTolerance:     5,
		},
		Input:  []uint64{1, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   0,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   2,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 3, 4, 3, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   2,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 3, 0, 4, 3, 1, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 3, 0, 4, 3, 1, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   2,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     1,
		},
		Input:  []uint64{1, 2, 3, 3, 0, 4, 3, 1, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 6, 4, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 0,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 6, 4, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 6, 5, 4, 6, 7, 5},
		Output: []uint64{1, 2, 3, 4, 5},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 6, 5, 4, 6, 7, 5},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     1,
		},
		Input:  []uint64{1, 2, 3, 5, 4, 6, 7, 5},
		Output: []uint64{1, 2, 3, 4},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   3,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 2, 4, 6, 6, 5, 6, 1, 1, 10, 1, 1, 10, 1, 10, 7},
		Output: []uint64{1, 2, 3, 4, 5, 6, 7},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   2,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 2, 4, 6, 6, 5, 6, 1, 1, 10, 1, 1, 10, 1, 10, 7},
		Output: []uint64{1, 2, 3},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   3,
			WrongBlocksTolerance: 1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 2, 4, 6, 6, 5, 6, 1, 1, 10, 1, 1, 10, 1, 10, 7},
		Output: []uint64{1, 2, 3, 4},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   3,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     8,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 2, 4, 6, 6, 5, 6, 1, 1, 10, 1, 1, 10, 1, 10, 7},
		Output: []uint64{1, 2, 3, 4, 5, 6, 7},
	},
	{
		Config: &Config{
			ReorderBufferSize:    0,
			LowBlocksTolerance:   3,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     7,
		},
		Input:  []uint64{1, 2, 3, 2, 3, 2, 4, 6, 6, 5, 6, 1, 1, 10, 1, 1, 10, 1, 10, 7},
		Output: []uint64{1, 2, 3, 4, 5, 6},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    1,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 3, 2},
		Output: []uint64{1, 2, 3},
	},
	{
		Config: &Config{
			ReorderBufferSize:    2,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 4, 3, 2},
		Output: []uint64{1, 2, 3, 4},
	},
	{
		Config: &Config{
			ReorderBufferSize:    1,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 4, 3, 2},
		Output: []uint64{1, 2, 3},
	},
	{
		Config: &Config{
			ReorderBufferSize:    2,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 4, 3, 2},
		Output: []uint64{1},
		Err:    drivers.ErrToleranceExceeded,
	},
	{
		Config: &Config{
			ReorderBufferSize:    2,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: 2,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 4, 3, 2},
		Output: []uint64{1, 2, 3, 4},
	},
	{
		Config: &Config{
			ReorderBufferSize:    10,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 10, 5, 7, 4, 8, 2, 3, 9, 6},
		Output: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	},
	{
		Config: &Config{
			ReorderBufferSize:    10,
			LowBlocksTolerance:   -1,
			WrongBlocksTolerance: -1,
			NoWriteTolerance:     -1,
		},
		Input:  []uint64{1, 10, 5, 7, 4, 8, 2, 3, 9, 6, 11, 12, 15, 13, 14},
		Output: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	},
}

func TestAligner(t *testing.T) {
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), tc.run)
	}
}
