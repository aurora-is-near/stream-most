package v2

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type BorealisUniqueID = [16]byte

type BorealisEnvelope struct {
	_            struct{} `json:"-" cbor:",toarray"`
	Type         uint16
	SequentialID uint64
	TimestampS   uint32
	TimestampMS  uint16
	UniqueID     BorealisUniqueID
}

var cborDecMode cbor.DecMode

func init() {
	var err error
	cborDecMode, err = cbor.DecOptions{
		MaxArrayElements: 2147483647,
	}.DecMode()
	if err != nil {
		panic(fmt.Errorf("unable to create CBOR decoding mode (borealis): %w", err))
	}
}

func DecodeBorealisPayload[T any](data []byte) (*T, error) {
	reader := bytes.NewReader(data)

	var err error
	var version byte
	if version, err = reader.ReadByte(); err != nil {
		return nil, err
	}

	switch version {
	case 1:
		decoder := cborDecMode.NewDecoder(reader)
		envelope := &BorealisEnvelope{}
		if err := decoder.Decode(envelope); err != nil {
			return nil, err
		}
		payload := new(T)
		if err := decoder.Decode(payload); err != nil {
			return nil, err
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unknown version of borealis-message: %v", version)
	}
}
