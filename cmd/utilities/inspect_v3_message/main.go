package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block/transaction"
	_ "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block/transaction/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
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

func hashToHex(h []byte) string {
	h2 := make([]byte, len(h)*2)
	_ = hex.Encode(h2, h)
	return string(h2)
}

func main() {
	startSeq := 0
	for i := 0; i < 100; i++ {
		startSeq += 1
		file := "./out/local/read_v3_message_" + strconv.Itoa(startSeq) + ".out"
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
			hash := hashToHex(msgT.NearBlockHeader.GetHeader().H256Hash)
			prevHash := hashToHex(msgT.NearBlockHeader.GetHeader().H256PrevHash)

			fmt.Printf("block hash %s\nprev hash %s\nchunks %v\n", hash, prevHash, msgT.NearBlockHeader.Header.ChunkMask)
		case *borealisproto.Message_NearBlockShard:
			fmt.Println("NearBlockShard")
			h := msgT.NearBlockShard.GetHeader().Header.H256Hash
			h2 := make([]byte, len(h)*2)
			_ = hex.Encode(h2, h)

			fmt.Printf("block hash %s, shard id %v\n", string(h2), msgT.NearBlockShard.ShardId)
		}
	}
}
