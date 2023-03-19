package main

import (
	"bytes"
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block/transaction"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block/transaction/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

func Decompress(in io.Reader, out io.Writer) error {
	d, err := zstd.NewReader(in)
	if err != nil {
		return err
	}
	defer d.Close()

	// Copy content...
	_, err = io.Copy(out, d)
	return err
}

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

func main() {
	file := os.Args[1]
	logrus.Info("Reading ", file)
	fileReader, err := os.Open(file)
	defer fileReader.Close()

	if err != nil {
		panic(err)
	}

	b, err := io.ReadAll(fileReader)
	if err != nil {
		panic(err)
	}

	msg, err := ProtoDecode(b)
	if err != nil {
		panic(err)
	}

	switch msgT := msg.Payload.(type) {
	case *borealisproto.Message_NearBlockHeader:
		fmt.Println("NearBlockHeader")
		fmt.Printf("%v\n", msgT.NearBlockHeader.GetHeader().)
	case *borealisproto.Message_NearBlockShard:
		fmt.Println("NearBlockShard", msgT.NearBlockShard.String())
	}
}
