package new_format

import (
	"bytes"
	"errors"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/klauspost/compress/zstd"
	"io"
)

func ProtoDecode(d []byte) (*borealisproto.Message, error) {
	if len(d) == 0 {
		return nil, io.EOF
	}
	decoded := new(bytes.Buffer)
	dec, err := zstd.NewReader(bytes.NewBuffer(d))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(decoded, dec); err != nil {
		return nil, err
	}
	dec.Close()
	msg := borealisproto.Message{}
	if err := msg.UnmarshalVT(decoded.Bytes()); err != nil {
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
		return messages.NewBlockAnnouncement(msgT), nil
	case *borealisproto.Message_NearBlockShard:
		return messages.NewBlockShard(msgT), nil
	}
	return nil, errors.New("cant parse message")
}
