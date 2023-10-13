package blocks

import (
	"strconv"
)

func ConstructMsgID(b Block) string {
	if b.GetBlockType() == Shard {
		return strconv.FormatUint(b.GetHeight(), 10) + "." + strconv.FormatUint(b.GetShardID(), 10)
	}
	return strconv.FormatUint(b.GetHeight(), 10)
}
