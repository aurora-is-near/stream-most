package v3

import (
	"bytes"
	"errors"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/klauspost/compress/zstd"
	"io"
)

func ProtoDecode(d []byte) (*borealisproto.Message, error) {
	/*
		A couple of things about `zstd.Decoder` behavior (yes, it's not obvious, but luckily for you - I already know this):
		1. It has a warm-up, meaning that reusing the same decoder is more efficient than recreating it
		2. It has async API (`io.Reader` conformant) and sync API (`dec.DecodeAll(buf)`)
		3. Async API only allows to decode one stream at a time (so it's not very convenient to reuse it concurrently)
		4. Sync API allows to decode a preconfigured number (N) of buffers simultaneously.
		   (N+1)'th concurrent call of `dec.DecodeAll(buf)` will be blocked until one of the previous calls is done and so on.
		   N can be configured by providing it: `zstd.NewReader(nil, zstd.WithDecoderConcurrency(N), ...)`.
		   By default N = `min(4, GOMAXPROCS)` (which is best optimized only for async usecase)

		Since your usage scenario doesn't need any asynchrony, the best way to use zstd decoding is:
		1. Define a single global instance of `zstd.Decoder``
		2. Protect initialization either by `init()` func, or by having a getter that utilizes `sync.Once`
		3. Initialization itself should look like this: `zstdDecoder = zstd.NewReader(nil, zstd.WithDecoderConcurrency(N), ...)`
		   Where N is the maximum expected concurrency.
		   Having N > GOMAXPROCS will decrease performance (more allocations and more warmups, but no benefit from it).
		   I recommend to:
		      - set it to `min(X, runtime.GOMAXPROCS(0))` if you know that you'll never have more than X concurrent calls.
			  - otherwise set it to `runtime.GOMAXPROCS(0)`.
		4. Use: `result, err := zstdDecoder.DecodeAll(src)`

		Reference: github.com/klauspost/compress/blob/master/zstd/README.md
	*/

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
		return messages.NewBlockAnnouncementV3(msgT), nil
	case *borealisproto.Message_NearBlockShard:
		return messages.NewBlockShard(msgT), nil
	/*
	default:
		return nil, fmt.Errorf("unexpected payload type: %T", decoded.Payload)
	*/
	}
	return nil, errors.New("cant parse message")
}
