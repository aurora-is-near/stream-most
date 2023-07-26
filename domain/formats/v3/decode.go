package v3

import (
	"fmt"
	"io"

	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/zstd"
)

func DecodeProto(d []byte) (*borealisproto.Message, error) {
	if len(d) == 0 {
		return nil, io.EOF
	}
	decoder := zstd.GetDecoder()
	decoded, err := decoder.DecodeAll(d, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to decompress zstd (borealisproto): %w", err)
	}
	msg := borealisproto.Message{}
	if err := msg.UnmarshalVT(decoded); err != nil {
		return nil, fmt.Errorf("unable to unmarshal protobuf of borealisproto message: %w", err)
	}

	return &msg, nil
}

func DecodeProtoBlock(d []byte) (blocks.Block, error) {
	decoded, err := DecodeProto(d)
	if err != nil {
		return nil, fmt.Errorf("unable to decode borealisproto message: %w", err)
	}

	switch msgT := decoded.Payload.(type) {

	case *borealisproto.Message_NearBlockHeader:
		switch {
		case msgT == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: payload (Message_NearBlockHeader) is nil")
		case msgT.NearBlockHeader == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: NearBlockHeader is nil")
		case msgT.NearBlockHeader.Header == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: NearBlockHeader.Header is nil")
		default:
			return NearBlockAnnouncement{BlockHeaderView: msgT.NearBlockHeader}, nil
		}

	case *borealisproto.Message_NearBlockShard:
		switch {
		case msgT == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: payload (Message_NearBlockShard) is nil")
		case msgT.NearBlockShard == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: NearBlockShard is nil")
		case msgT.NearBlockShard.Header == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: NearBlockShard.Header is nil")
		case msgT.NearBlockShard.Header.Header == nil:
			return nil, fmt.Errorf("unable to decode borealisproto message: NearBlockShard.Header.Header is nil")
		default:
			return NearBlockShard{BlockShard: msgT.NearBlockShard}, nil
		}

	default:
		return nil, fmt.Errorf("unable to decode borealisproto message: unexpected payload type: %T", decoded.Payload)
	}
}
