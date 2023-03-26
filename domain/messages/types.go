package messages

type MessageType uint64

const (
	Announcement MessageType = iota
	Shard
)
