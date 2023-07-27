package u

import (
	"fmt"

	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	near_block "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go"
)

/*
	Those functions are only used in tests.
	Removing this file will not affect anything but tests
*/

func Announcement(sequence uint64, shardsMap []bool, height uint64, hash string, prevHash string) messages.Message {
	return BTN(sequence, NewSimpleBlockAnnouncement(shardsMap, height, hash, prevHash))
}

func Shard(sequence uint64, height uint64, hash string, prevHash string, shardId uint64) messages.Message {
	return BTN(sequence, NewSimpleBlockShard(height, hash, prevHash, shardId))
}

func BTN(sequence uint64, block blocks.Block) messages.Message {
	return BlockToNats(sequence, block)
}

func BlockToNats(sequence uint64, block blocks.Block) messages.Message {
	data, err := RecoverBlockPayload(block)
	if err != nil {
		panic(err)
	}

	return &messages.AbstractRawMessage{
		TypedMessage: messages.TypedMessage{
			Block: block,
		},
		RawMsg: &nats.RawStreamMsg{
			Sequence: sequence,
			Header:   make(nats.Header),
			Data:     data,
		},
	}
}

// BuildMessageToRawStreamMsg converts a message to a RawStreamMsg.
// If message itself doesn't have Data []byte (most likely hand-crafted object),
// then it will be created manually
func BuildMessageToRawStreamMsg(message messages.Message) *nats.RawStreamMsg {
	var err error
	data := message.GetData()
	if len(data) == 0 {
		if data, err = RecoverBlockPayload(message.GetBlock()); err != nil {
			panic(err)
		}
	}
	return &nats.RawStreamMsg{
		Subject:  message.GetSubject(),
		Sequence: message.GetSequence(),
		Header:   message.GetHeader(),
		Data:     data,
		Time:     message.GetTimestamp(),
	}
}

func NewSimpleBlockAnnouncement(shardsMap []bool, height uint64, hash string, prevHash string) blocks.BlockAnnouncement {
	return &v3.NearBlockAnnouncement{
		BlockHeaderView: &near_block.BlockHeaderView{
			Header: &near_block.IndexerBlockHeaderView{
				Height:       height,
				H256Hash:     []byte(hash),
				H256PrevHash: []byte(prevHash),
				ChunkMask:    shardsMap,
			},
		},
	}
}

func NewSimpleBlockShard(height uint64, hash string, prevHash string, shardId uint64) blocks.BlockShard {
	return &v3.NearBlockShard{
		BlockShard: &near_block.BlockShard{
			ShardId: shardId,
			Header: &near_block.PartialBlockHeaderView{
				Header: &near_block.PartialIndexerBlockHeaderView{
					Height:       height,
					H256Hash:     []byte(hash),
					H256PrevHash: []byte(prevHash),
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
