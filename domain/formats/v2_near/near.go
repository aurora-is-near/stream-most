package v2_near

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	v2 "github.com/aurora-is-near/stream-most/domain/formats/v2"
	"github.com/buger/jsonparser"
	"github.com/nats-io/nats.go"
	"github.com/pierrec/lz4/v4"
)

var nearBlockSchema = [][]string{
	{"block", "header", "hash"},
	{"block", "header", "prev_hash"},
	{"block", "header", "height"},
}

func DecodeNearBlockJSON(data []byte) ([]byte, error) {
	payload, err := v2.DecodeBorealisPayload[[]byte](data)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, lz4.NewReader(bytes.NewReader(*payload))); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeNearBlock(data []byte, _ nats.Header) (*blocks.AbstractBlock, error) {
	blockJSON, err := DecodeNearBlockJSON(data)
	if err != nil {
		return nil, err
	}

	block := &blocks.AbstractBlock{}

	var anyErr error
	jsonparser.EachKey(
		blockJSON,
		func(i int, b []byte, vt jsonparser.ValueType, err error) {
			if anyErr != nil {
				return
			}
			if err != nil {
				anyErr = err
				return
			}
			switch i {
			case 0:
				if vt != jsonparser.String {
					anyErr = fmt.Errorf("block.header.hash must be string")
					return
				}
				block.Hash = string(b)
			case 1:
				if vt != jsonparser.String {
					anyErr = fmt.Errorf("block.header.prev_hash must be string")
					return
				}
				block.PrevHash = string(b)
			case 2:
				if vt != jsonparser.Number {
					anyErr = fmt.Errorf("block.header.height must be number")
					return
				}
				block.Height, err = strconv.ParseUint(string(b), 10, 64)
				if err != nil {
					anyErr = fmt.Errorf("unable to parse block.header.height: %v", err)
					return
				}
			}
		},
		nearBlockSchema...,
	)

	if anyErr != nil {
		return nil, anyErr
	}

	return block, nil
}
