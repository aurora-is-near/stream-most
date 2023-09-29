package u

import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/stretchr/testify/require"
)

func ExtractState(sp blockio.StateProvider) (lastKnownDeletedSeq uint64, lastKnownSeq uint64, lastKnownMsg blockio.Msg, err error) {
	if lastKnownDeletedSeq, err = sp.LastKnownDeletedSeq(); err != nil {
		return
	}
	if lastKnownSeq, err = sp.LastKnownSeq(); err != nil {
		return
	}
	if lastKnownMsg, err = sp.LastKnownMessage(); err != nil {
		return
	}
	return
}

func RequireSeq(t *testing.T, actualSeq uint64, minAllowedSeq uint64, expectedSeq uint64) bool {
	require.GreaterOrEqual(t, actualSeq, minAllowedSeq)
	require.LessOrEqual(t, actualSeq, expectedSeq)
	return actualSeq == expectedSeq
}

func RequireLastKnownMsg(t *testing.T, target blockio.Msg, msgs ...*messages.BlockMessage) bool {
	oneof, last := false, false

	for _, msg := range msgs {
		last = matchLastKnownMsg(t, target, msg)
		oneof = oneof || last
	}

	require.True(t, oneof)
	return last
}

func matchLastKnownMsg(t *testing.T, actual blockio.Msg, required *messages.BlockMessage) bool {
	if actual == nil && required == nil {
		return true
	}
	if actual == nil || required == nil {
		return false
	}

	require.NotNil(t, actual.Get())
	if actual.Get().GetSequence() != required.Msg.GetSequence() {
		return false
	}

	require.Equal(t, required.Msg.GetData(), actual.Get().GetData())

	blockMsg, err := actual.GetDecoded(context.Background())
	require.NoError(t, err)
	require.NotNil(t, blockMsg)
	require.NotNil(t, blockMsg.Msg)
	require.Equal(t, required.Msg.GetData(), blockMsg.Msg.GetData())
	require.Equal(t, required.Msg.GetSequence(), blockMsg.Msg.GetSequence())
	require.NotNil(t, blockMsg.Block)
	require.Equal(t, required.Block.GetBlockType(), blockMsg.Block.GetBlockType())
	require.Equal(t, required.Block.GetHeight(), blockMsg.Block.GetHeight())
	require.Equal(t, required.Block.GetShardID(), blockMsg.Block.GetShardID())
	require.Equal(t, required.Block.GetHash(), blockMsg.Block.GetHash())
	require.Equal(t, required.Block.GetPrevHash(), blockMsg.Block.GetPrevHash())
	require.Equal(t, required.Block.GetShardMask(), blockMsg.Block.GetShardMask())

	return true
}
