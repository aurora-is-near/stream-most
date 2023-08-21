package blocks

type BlockType uint64

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
	Announcement BlockType = iota
	Shard
	Unknown
)
