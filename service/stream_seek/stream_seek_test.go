package stream_seek

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support"
	"testing"
)

func TestStreamSeek_SeekShards(t *testing.T) {
	testInput := stream.NewFakeNearV3Stream()
	testInput.Add(
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		support.STN(3, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
	)

	seeker := NewStreamSeek(testInput)
	shards, err := seeker.SeekShards(1, 3, &[]string{"AAA"}[0])
	if err != nil {
		t.Error(err)
	}

	if len(shards) != 2 {
		t.Error("bad count of shards")
	}

	for _, v := range shards {
		if !v.IsShard() {
			t.Error("not a shard")
		}
		if !(v.GetSequence() == 2) && !(v.GetSequence() == 3) {
			t.Error("bad sequence")
		}
	}
}
