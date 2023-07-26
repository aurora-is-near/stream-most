package v2

import (
	"fmt"

	"github.com/aurora-is-near/borealis.go"
	"github.com/fxamacker/cbor/v2"
)

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
	var msg borealis.BusMessage
	msg.OverrideEventFactory(func(messageType borealis.MessageType) any {
		return new(T)
	})
	if err := msg.DecodeCBORWithMode(data, cborDecMode); err != nil {
		return nil, fmt.Errorf("unable to decode CBOR borealis message: %w", err)
	}
	return msg.PayloadPtr.(*T), nil
}
