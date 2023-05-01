package zstd

import (
	"github.com/klauspost/compress/zstd"
	"runtime"
	"sync"
)

var once sync.Once
var decoder *zstd.Decoder

func GetDecoder() *zstd.Decoder {
	once.Do(func() {
		var err error
		decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
		if err != nil {
			panic("Cannot initialize the zstd decoder")
		}
	})

	return decoder
}
