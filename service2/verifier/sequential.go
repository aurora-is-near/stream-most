package verifier

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Sequential struct {
	AllowHeightGaps       bool
	CheckBlocksCompletion bool
	CheckHashes           bool
	ShardFilter           []bool
}

func (s *Sequential) CanAppend(last, next *messages.BlockMessage) error {
	if next.Block.GetBlockType() == blocks.Shard && !s.checkShardFilter(next.Block.GetShardID()) {
		return ErrFilteredShard
	}

	if last == nil {
		return nil
	}

	if next.Block.GetHeight() < last.Block.GetHeight() {
		return ErrLowHeight
	}

	if next.Block.GetHeight() > last.Block.GetHeight() {
		if s.CheckBlocksCompletion && !s.isCompleteBlock(last) {
			return ErrIncompletePreviousBlock
		}
		if next.Block.GetBlockType() != blocks.Announcement {
			return ErrUnannouncedBlock
		}
		if !s.AllowHeightGaps && next.Block.GetHeight()-last.Block.GetHeight() > 1 {
			return ErrHeightGap
		}
		if s.CheckHashes && next.Block.GetPrevHash() != last.Block.GetHash() {
			return ErrHashMismatch
		}
		return nil
	}

	if next.Block.GetBlockType() == blocks.Announcement {
		return ErrReannouncement
	}
	if next.Block.GetBlockType() != blocks.Shard {
		return ErrUnknownBlockType
	}

	if last.Block.GetBlockType() == blocks.Shard && next.Block.GetShardID() <= last.Block.GetShardID() {
		return ErrLowShard
	}
	if s.CheckBlocksCompletion {
		nextNeededShardID, hasRemainingShards := s.getNextNeededShardID(last)
		if !hasRemainingShards {
			return ErrUnwantedShard
		}
		if next.Block.GetShardID() < nextNeededShardID {
			return ErrLowShard
		}
		if next.Block.GetShardID() > nextNeededShardID {
			return ErrShardGap
		}
	}

	return nil
}

func (s *Sequential) checkShardFilter(shardID uint64) bool {
	if s.ShardFilter == nil {
		return true
	}
	if uint64(len(s.ShardFilter)) <= shardID {
		return false
	}
	return s.ShardFilter[int(shardID)]
}

func (s *Sequential) isCompleteBlock(b *messages.BlockMessage) bool {
	_, hasRemainingShards := s.getNextNeededShardID(b)
	return !hasRemainingShards
}

func (s *Sequential) getNextNeededShardID(b *messages.BlockMessage) (uint64, bool) {
	mask := b.Block.GetShardMask()

	start := 0
	if b.Block.GetBlockType() == blocks.Shard {
		if b.Block.GetShardID() >= uint64(len(mask)) {
			return 0, false
		}
		start = int(b.Block.GetShardID()) + 1
	}

	for i := start; i < len(mask); i++ {
		if mask[i] && s.checkShardFilter(uint64(i)) {
			return uint64(i), true
		}
	}

	return 0, false
}
