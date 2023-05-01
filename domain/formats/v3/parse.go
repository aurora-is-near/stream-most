package v3

import (
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/domain/zstd"
	"io"
)

func ProtoDecode(d []byte) (*borealisproto.Message, error) {
	if len(d) == 0 {
		return nil, io.EOF
	}
	decoder := zstd.GetDecoder()
	decoded, err := decoder.DecodeAll(d, nil)
	if err != nil {
		return nil, err
	}
	msg := borealisproto.Message{}
	if err := msg.UnmarshalVT(decoded); err != nil {
		return nil, err
	}

	return &msg, nil
}

func ProtoToMessage(d []byte) (interface{}, error) {
	decoded, err := ProtoDecode(d)
	if err != nil {
		return nil, err
	}

	switch msgT := decoded.Payload.(type) {
	case *borealisproto.Message_NearBlockHeader:
		return messages.NewBlockAnnouncementV3(msgT), nil
	case *borealisproto.Message_NearBlockShard:
		return messages.NewBlockShard(msgT), nil
	default:
		return nil, fmt.Errorf("unexpected payload type: %T", decoded.Payload)
	}
}
