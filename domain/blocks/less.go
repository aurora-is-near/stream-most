package blocks

func Less(x, y Block) bool {
	if x.GetHeight() < y.GetHeight() {
		return true
	}
	if x.GetHeight() > y.GetHeight() {
		return false
	}
	switch x.GetBlockType() {
	case Announcement:
		return y.GetBlockType() != Announcement
	case Shard:
		switch y.GetBlockType() {
		case Announcement:
			return false
		case Shard:
			return x.GetShardID() < y.GetShardID()
		default:
			return true
		}
	default:
		return false
	}
}
