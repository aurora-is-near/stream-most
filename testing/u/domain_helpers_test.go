package u

import (
	"testing"

	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
)

// TestBuildMessageToRawStreamMsg tests that BuildMessageToRawStreamMsg correctly converts
// a data-less message to a RawStreamMsg.
func TestBuildMessageToRawStreamMsg(t *testing.T) {
	type args struct {
		message messages.NatsMessage
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "announcement",
			args: args{
				message: AnnouncementToNats(100, NewSimpleBlockAnnouncement(
					[]bool{true, true, true}, 1, "hash", "prev_hash"),
				),
			},
		},
		{
			name: "shard",
			args: args{
				message: ShardToNats(100, NewSimpleBlockShard(
					[]bool{true, true, true}, 1, "hash", "prev_hash", 1,
				)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildMessageToRawStreamMsg(tt.args.message)

			decode, err := v3.DecodeProto(got.Data)
			if err != nil {
				panic(err)
			}

			switch msg := decode.Payload.(type) {
			case *borealisproto.Message_NearBlockHeader:
				if !tt.args.message.IsAnnouncement() {
					t.Errorf("Type decode failed")
				}
				if string(msg.NearBlockHeader.GetHeader().H256Hash) != tt.args.message.GetAnnouncement().Block.Hash {
					t.Errorf("Hash decode failed")
				}
			case *borealisproto.Message_NearBlockShard:
				if !tt.args.message.IsShard() {
					t.Errorf("Type decode failed")
				}
				if string(msg.NearBlockShard.GetHeader().Header.H256Hash) != tt.args.message.GetShard().Block.Hash {
					t.Errorf("Hash decode failed")
				}
			}
		})
	}
}
