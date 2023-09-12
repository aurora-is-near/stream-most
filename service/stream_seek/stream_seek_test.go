package stream_seek

import (
	"testing"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/aurora-is-near/stream-most/util"
)

func TestStreamSeek_SeekLastFullyWrittenBlock(t *testing.T) {
	formats.UseFormat(formats.NearV3)
	testInput := fake.NewStream()
	testInput.Add(
		u.Announcement(1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(2, 1, 0, "AAA", "000", []bool{true, true, true}),
		u.Shard(3, 1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(4, 1, 2, "AAA", "000", []bool{true, true, true}),
	)

	seeker := NewStreamSeek(testInput)
	announcement, shards, err := seeker.SeekLastFullyWrittenBlock()
	if err != nil {
		t.Fatal(err)
	}

	if announcement.Block.GetBlockType() != blocks.Announcement {
		t.Error("not an announcement")
	}

	if announcement.Msg.GetSequence() != 1 {
		t.Error("bad sequence")
	}

	println(len(shards))
}

func TestStreamSeek_SeekShards(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	testInput := fake.NewStream()
	testInput.Add(
		u.Announcement(1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(2, 1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(3, 1, 2, "AAA", "000", []bool{true, true, true}),
		u.Shard(4, 1, 2, "AAA", "000", []bool{true, true, true}),
	)

	seeker := NewStreamSeek(testInput)
	shards, err := seeker.SeekShards(1, 3, util.Ptr("AAA"))
	if err != nil {
		t.Fatal(err)
	}

	if len(shards) != 2 {
		t.Error("bad count of shards")
	}

	for _, v := range shards {
		if v.Block.GetBlockType() != blocks.Shard {
			t.Error("not a shard")
		}
		if !(v.Msg.GetSequence() == 2) && !(v.Msg.GetSequence() == 3) {
			t.Error("bad sequence")
		}
	}
}
