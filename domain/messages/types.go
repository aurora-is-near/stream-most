package messages

type MessageType uint64

func (t MessageType) String() interface{} {
	switch t {
	case Announcement:
		return "[A]"
	case Shard:
		return "[S]"
	}
	return "[Unknown]"
}

const (
	Announcement MessageType = iota
	Shard
)
