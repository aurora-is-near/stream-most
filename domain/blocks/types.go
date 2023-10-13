package blocks

type BlockType int

func (t BlockType) String() string {
	switch t {
	case Announcement:
		return "[H]"
	case Shard:
		return "[B]"
	default:
		return "Unknown"
	}
}

const (
	Announcement BlockType = 0
	Shard        BlockType = 1
	Unknown      BlockType = 2
)
