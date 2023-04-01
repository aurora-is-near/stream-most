package stream_seek

import (
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestStreamSeek_SeekShards(t *testing.T) {
	testInput := fake.NewStream()
	testInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 1),
		u.Shard(3, 1, "AAA", "000", 2),
		u.Shard(4, 1, "AAA", "000", 2),
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
