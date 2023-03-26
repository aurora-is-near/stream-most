package new_format

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

	buf := make([]byte, 0)
	out := bytes.NewBuffer(buf)

	writer, err := zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.SpeedDefault))
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
