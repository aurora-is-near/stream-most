package u

import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/stretchr/testify/require"
)

func RequireEmptyState(t *testing.T, state blockio.State) {
	require.Less(t, state.FirstSeq(), uint64(2))
	require.Equal(t, uint64(0), state.LastSeq())
	require.Nil(t, state.Tip())
}

func RequireState(t *testing.T, state blockio.State, firstSeq uint64, lastSeq uint64, blockType blocks.BlockType, height uint64, shardID uint64, hash string, prevHash string, shardMask []bool) {
	require.Equal(t, firstSeq, state.FirstSeq())
	require.Equal(t, lastSeq, state.LastSeq())
	require.NotNil(t, state.Tip())
	require.NotNil(t, state.Tip().Get())
	require.NotNil(t, state.Tip().Get().GetData())
	require.Positive(t, len(state.Tip().Get().GetData()))
	require.Equal(t, lastSeq, state.Tip().Get().GetSequence())
	blockMsg, err := state.Tip().GetDecoded(context.Background())
	require.NoError(t, err)
	require.NotNil(t, blockMsg)
	require.NotNil(t, blockMsg.Msg)
	require.NotNil(t, blockMsg.Msg.GetData())
	require.Positive(t, len(blockMsg.Msg.GetData()))
	require.Equal(t, lastSeq, blockMsg.Msg.GetSequence())
	require.NotNil(t, blockMsg.Block)
	require.Equal(t, blockType, blockMsg.Block.GetBlockType())
	require.Equal(t, height, blockMsg.Block.GetHeight())
	require.Equal(t, shardID, blockMsg.Block.GetShardID())
	require.Equal(t, hash, blockMsg.Block.GetHash())
	require.Equal(t, prevHash, blockMsg.Block.GetPrevHash())
	require.Equal(t, shardMask, blockMsg.Block.GetShardMask())
}
