package u

import (
	"fmt"

	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	near_block "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go/jetstream"
)

/*
	Those functions are only used in tests.
	Removing this file will not affect anything but tests
*/

func Announcement(sequence uint64, height uint64, hash string, prevHash string, shardMask []bool) *messages.BlockMessage {
	return MessageFromBlock(sequence, NewSimpleBlockAnnouncement(height, hash, prevHash, shardMask))
}

func Shard(sequence uint64, height uint64, shardId uint64, hash string, prevHash string, shardMask []bool) *messages.BlockMessage {
	return MessageFromBlock(sequence, NewSimpleBlockShard(height, shardId, hash, prevHash, shardMask))
}

func MessageFromBlock(sequence uint64, block blocks.Block) *messages.BlockMessage {
	data, err := RecoverBlockPayload(block)
	if err != nil {
		panic(err)
	}

	return &messages.BlockMessage{
		Block: block,
		Msg: messages.RawStreamMessage{
			RawStreamMsg: &jetstream.RawStreamMsg{
				Sequence: sequence,
				Data:     data,
			},
		},
	}
}

func NewSimpleBlockAnnouncement(height uint64, hash string, prevHash string, shardMask []bool) *v3.NearBlockAnnouncement {
	return &v3.NearBlockAnnouncement{
		BlockHeaderView: &near_block.BlockHeaderView{
			Header: &near_block.IndexerBlockHeaderView{
				Height:       height,
				H256Hash:     []byte(hash),
				H256PrevHash: []byte(prevHash),
				ChunkMask:    shardMask,
			},
		},
	}
}

func NewSimpleBlockShard(height uint64, shardId uint64, hash string, prevHash string, shardMask []bool) *v3.NearBlockShard {
	return &v3.NearBlockShard{
		BlockShard: &near_block.BlockShard{
			ShardId: shardId,
			Header: &near_block.PartialBlockHeaderView{
				Header: &near_block.PartialIndexerBlockHeaderView{
					Height:       height,
					H256Hash:     []byte(hash),
					H256PrevHash: []byte(prevHash),
					ChunkMask:    shardMask,
				},
			},
		},
	}
}

func RecoverBlockPayload(block blocks.Block) ([]byte, error) {
	if block, ok := block.(*v3.NearBlockAnnouncement); ok {
		return v3.ProtoEncode(&borealisproto.Message{
			Payload: &borealisproto.Message_NearBlockHeader{
				NearBlockHeader: block.BlockHeaderView,
			},
		})
	}
	if block, ok := block.(*v3.NearBlockShard); ok {
		return v3.ProtoEncode(&borealisproto.Message{
			Payload: &borealisproto.Message_NearBlockShard{
				NearBlockShard: block.BlockShard,
			},
		})
	}
	return nil, fmt.Errorf("unable to recover payload from block type %T", block)
}
