package messages

type MessageType uint64

func (t MessageType) String() string {
	switch t {
	case Announcement:
		return "[H]"
	case Shard:
		return "[B]"
	}
	return "[Unknown]"
}

const (
	Announcement MessageType = iota
	Shard
)
