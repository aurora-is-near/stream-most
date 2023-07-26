package u

import (
	"time"

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

func Announcement(sequence uint64, shardsMap []bool, height uint64, hash string, prevHash string) messages.NatsMessage {
	return ATN(sequence, NewSimpleBlockAnnouncement(shardsMap, height, hash, prevHash))
}

func Shard(sequence uint64, height uint64, hash string, prevHash string, shardId uint64) messages.NatsMessage {
	return STN(sequence, NewSimpleBlockShard([]bool{}, height, hash, prevHash, shardId))
}

func ATN(sequence uint64, msg *messages.BlockAnnouncement) messages.NatsMessage {
	return AnnouncementToNats(sequence, msg)
}

func AnnouncementToNats(sequence uint64, msg *messages.BlockAnnouncement) messages.NatsMessage {
	m := &borealisproto.Message{
		Payload: msg.Parent,
	}
	data, err := v3.ProtoEncode(m)
	if err != nil {
		panic(err)
	}

	return messages.NatsMessage{
		Msg: &nats.Msg{
			Header: map[string][]string{},
			Data:   data,
		},
		Announcement: msg,
		Metadata: &nats.MsgMetadata{Sequence: nats.SequencePair{
			Stream: sequence,
		}},
	}
}

func STN(sequence uint64, msg *messages.BlockShard) messages.NatsMessage {
	return ShardToNats(sequence, msg)
}

func ShardToNats(sequence uint64, msg *messages.BlockShard) messages.NatsMessage {
	m := &borealisproto.Message{
		Payload: msg.Parent,
	}
	data, err := v3.ProtoEncode(m)
	if err != nil {
		panic(err)
	}

	return messages.NatsMessage{
		Msg: &nats.Msg{
			Header: map[string][]string{},
			Data:   data,
		},
		Shard: msg,
		Metadata: &nats.MsgMetadata{Sequence: nats.SequencePair{
			Stream:   sequence,
			Consumer: sequence,
		}},
	}
}

// BuildMessageToRawStreamMsg converts a message to a RawStreamMsg.
// If message itself doesn't have Data []byte (most likely hand-crafted object),
// then it will be created manually
func BuildMessageToRawStreamMsg(message messages.NatsMessage) *nats.RawStreamMsg {
	var err error
	data := message.Msg.Data
	if len(data) == 0 {
		if message.IsShard() {
			m := &borealisproto.Message{
				Payload: message.GetShard().Parent,
			}
			data, err = v3.ProtoEncode(m)
			if err != nil {
				panic(err)
			}
		} else {
			m := &borealisproto.Message{
				Payload: message.GetAnnouncement().Parent,
			}
			data, err = v3.ProtoEncode(m)
			if err != nil {
				panic(err)
			}
		}
	}
	return &nats.RawStreamMsg{
		Subject:  "",
		Sequence: message.GetSequence(),
		Header:   map[string][]string{},
		Data:     data,
		Time:     time.Time{},
	}
}

func NewSimpleBlockAnnouncement(shardsMap []bool, height uint64, hash string, prevHash string) *messages.BlockAnnouncement {
	return &messages.BlockAnnouncement{
		Parent: &borealisproto.Message_NearBlockHeader{
			NearBlockHeader: &near_block.BlockHeaderView{
				Header: &near_block.IndexerBlockHeaderView{
					Height:       height,
					H256Hash:     []byte(hash),
					H256PrevHash: []byte(prevHash),
					ChunkMask:    shardsMap,
				},
			},
		},
		Block: blocks.AbstractBlock{
			Hash:     hash,
			PrevHash: prevHash,
			Height:   height,
		},
		ParticipatingShardsMap: shardsMap,
	}
}

func NewSimpleBlockShard(shardsMap []bool, height uint64, hash string, prevHash string, shardId uint64) *messages.BlockShard {
	return &messages.BlockShard{
		Parent: &borealisproto.Message_NearBlockShard{
			NearBlockShard: &near_block.BlockShard{
				Header: &near_block.PartialBlockHeaderView{
					Header: &near_block.PartialIndexerBlockHeaderView{
						Height:       height,
						H256Hash:     []byte(hash),
						H256PrevHash: []byte(prevHash),
						ChunkMask:    shardsMap,
					},
				},
				ShardId: shardId,
			},
		},
		Block: blocks.AbstractBlock{
			Hash:     hash,
			PrevHash: prevHash,
			Height:   height,
		},
		ShardID: uint8(shardId),
	}
}
