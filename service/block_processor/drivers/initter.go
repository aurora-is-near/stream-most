package drivers

import (
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
)

func Infer(tp DriverType, input, output stream.Interface) Driver {
	switch tp {
	case NearV3:
		var lastWrittenHash *string
		lastWrittenBlock, _, err := stream_seek.NewStreamSeek(output).SeekLastFullyWrittenBlock()
		if err != nil {
			if err != stream_seek.ErrNotFound {
				panic(err)
			}
		} else {
			lastWrittenHash = &lastWrittenBlock.GetBlock().Hash
		}

		return near_v3.NewNearV3((&near_v3.Options{
			StuckTolerance:          5,
			StuckRecovery:           true,
			StuckRecoveryWindowSize: 10,
			LastWrittenBlockHash:    lastWrittenHash,
			BlocksCacheSize:         10,
		}).Validated())
	default:
		panic("Unknown driver")
	}
}
