package u

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/stretchr/testify/require"
)

type SeqRange struct {
	Min uint64
	Max uint64
}

func (s *SeqRange) Match(t *testing.T, seq uint64) (final bool) {
	require.GreaterOrEqual(t, seq, s.Min)
	require.LessOrEqual(t, seq, s.Max)
	return seq == s.Max
}

type ExpectedState struct {
	LastDeletedSeq SeqRange
	LastSeq        SeqRange
	LastMsg        []*messages.BlockMessage
	AllowedErrors  []error
	RequiredErrors []error
}

func (s *ExpectedState) MatchError(t *testing.T, err error) (final bool) {
	for _, rErr := range s.RequiredErrors {
		if errors.Is(err, rErr) {
			return true
		}
	}
	for _, aErr := range s.AllowedErrors {
		if errors.Is(err, aErr) {
			return false
		}
	}
	require.NoError(t, err)
	return false
}

func (s *ExpectedState) MatchLastDeletedSeq(t *testing.T, sp blockio.StateProvider) (final bool) {
	x, err := sp.LastKnownDeletedSeq()
	if err != nil {
		return s.MatchError(t, err)
	}
	return s.LastDeletedSeq.Match(t, x) && len(s.RequiredErrors) == 0
}

func (s *ExpectedState) MatchLastSeq(t *testing.T, sp blockio.StateProvider) (final bool) {
	x, err := sp.LastKnownSeq()
	if err != nil {
		return s.MatchError(t, err)
	}
	return s.LastSeq.Match(t, x) && len(s.RequiredErrors) == 0
}

func (s *ExpectedState) MatchLastMsg(t *testing.T, sp blockio.StateProvider) (final bool) {
	msg, seq, err := sp.LastKnownMessage()
	if err != nil {
		return s.MatchError(t, err)
	}
	if msg != nil {
		require.NotNil(t, msg.Get())
		require.Equal(t, msg.Get().GetSequence(), seq)
	}
	msgFinal := matchLastMsg(t, msg, s.LastMsg...)
	seqFinal := s.LastSeq.Match(t, seq)
	return msgFinal && seqFinal && len(s.RequiredErrors) == 0
}

func MatchState(t *testing.T, sp blockio.StateProvider, s *ExpectedState) (final bool) {
	ld := s.MatchLastDeletedSeq(t, sp)
	ls := s.MatchLastSeq(t, sp)
	lm := s.MatchLastMsg(t, sp)
	return ld && ls && lm
}

func MatchFinalState(t *testing.T, sp blockio.StateProvider, s *ExpectedState) {
	require.True(t, MatchState(t, sp, s))
}

func WaitState(t *testing.T, timeout time.Duration, sp blockio.StateProvider, s *ExpectedState) {
	require.Eventually(t, func() bool {
		return MatchState(t, sp, s)
	}, timeout, time.Second/20)
}

func matchLastMsg(t *testing.T, target blockio.Msg, msgs ...*messages.BlockMessage) bool {
	oneof, last := false, false

	for _, msg := range msgs {
		last = compareLastMsg(t, target, msg)
		oneof = oneof || last
	}

	require.True(t, oneof)
	return last
}

func compareLastMsg(t *testing.T, actual blockio.Msg, expected *messages.BlockMessage) bool {
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	require.NotNil(t, actual.Get())
	if actual.Get().GetSequence() != expected.Msg.GetSequence() {
		return false
	}

	require.Equal(t, expected.Msg.GetData(), actual.Get().GetData())

	blockMsg, err := actual.GetDecoded(context.Background())
	require.NoError(t, err)
	require.NotNil(t, blockMsg)
	require.NotNil(t, blockMsg.Msg)
	require.Equal(t, expected.Msg.GetData(), blockMsg.Msg.GetData())
	require.Equal(t, expected.Msg.GetSequence(), blockMsg.Msg.GetSequence())
	require.NotNil(t, blockMsg.Block)
	require.Equal(t, expected.Block.GetBlockType(), blockMsg.Block.GetBlockType())
	require.Equal(t, expected.Block.GetHeight(), blockMsg.Block.GetHeight())
	require.Equal(t, expected.Block.GetShardID(), blockMsg.Block.GetShardID())
	require.Equal(t, expected.Block.GetHash(), blockMsg.Block.GetHash())
	require.Equal(t, expected.Block.GetPrevHash(), blockMsg.Block.GetPrevHash())
	require.Equal(t, expected.Block.GetShardMask(), blockMsg.Block.GetShardMask())

	return true
}
