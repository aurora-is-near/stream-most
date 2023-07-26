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
		message messages.Message
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "announcement",
			args: args{
				message: BlockToNats(100, NewSimpleBlockAnnouncement(
					[]bool{true, true, true}, 1, "hash", "prev_hash"),
				),
			},
		},
		{
			name: "shard",
			args: args{
				message: BlockToNats(100, NewSimpleBlockShard(
					1, "hash", "prev_hash", 1,
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
				if tt.args.message.GetType() != messages.Announcement {
					t.Errorf("Type decode failed")
				}
				if string(msg.NearBlockHeader.GetHeader().H256Hash) != tt.args.message.GetHash() {
					t.Errorf("Hash decode failed")
				}
			case *borealisproto.Message_NearBlockShard:
				if tt.args.message.GetType() != messages.Shard {
					t.Errorf("Type decode failed")
				}
				if string(msg.NearBlockShard.GetHeader().Header.H256Hash) != tt.args.message.GetHash() {
					t.Errorf("Hash decode failed")
				}
			}
		})
	}
}
