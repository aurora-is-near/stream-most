package v3

import (
	"bytes"
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	zstd "github.com/klauspost/compress/zstd"
)

func ProtoEncode(message *borealisproto.Message) ([]byte, error) {
	/*
		A couple of things about `zstd.Encoder` behavior (yes, it's not obvious, but luckily for you - I already know this):
		1. It has a warm-up, meaning that reusing the same encoder is more efficient than recreating it
		2. It has async API (`io.WriteCloser` conformant) and sync API (`enc.EncodeAll(src, dst)`)
		3. Async API only allows to encode one stream at a time (so it's not very convenient to reuse it concurrently)
		4. Sync API allows to encode a preconfigured number (N) of buffers simultaneously.
		   (N+1)'th concurrent call of `enc.EncodeAll(src, dst)` will be blocked until one of the previous calls is done and so on.
		   N can be configured by providing it: `zstd.NewWriter(nil, zstd.WithEncoderConcurrency(N), ...)`.
		   By default N = `GOMAXPROCS`

		Since your usage scenario doesn't need any asynchrony, the best way to use zstd decoding is:
		1. Define a single global instance of `zstd.Encoder``
		2. Protect initialization either by `init()` func, or by having a getter that utilizes `sync.Once`
		3. Initialization itself should look like this: `zstdEncoder = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(N), ...)`
		   Where N is the maximum expected concurrency.
		   Having N > GOMAXPROCS will decrease performance (more allocations and more warmups, but no benefit from it).
		   I recommend to:
		      - set it to `min(X, runtime.GOMAXPROCS(0))` if you know that you'll never have more than X concurrent calls.
			  - otherwise set it to `runtime.GOMAXPROCS(0)`.
		4. Use it: `result := zstdEncoder.EncodeAll(src, nil)`

		Reference: github.com/klauspost/compress/blob/master/zstd/README.md
	*/

	data, err := message.MarshalVT()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0)
	out := bytes.NewBuffer(buf) // Can be simpilified: `var out bytes.Buffer`

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
