package v3

import (
	"bytes"

	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	zstd "github.com/klauspost/compress/zstd"
)

func ProtoEncode(message *borealisproto.Message) ([]byte, error) {
	data, err := message.MarshalVT()
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer

	writer, err := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(data)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}
