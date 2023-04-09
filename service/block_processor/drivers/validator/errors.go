package validator

import "errors"

var (
	ErrHeightUnordered    = errors.New("found block with height less than or equal to previous blocks")
	ErrPrecedingShards    = errors.New("shards are preceding their respective announcement")
	ErrIncompleteBlock    = errors.New("previous announcement's block is not complete, new announcement received")
	ErrUnknownMessageType = errors.New("unknown message type")
	ErrUndesiredShard     = errors.New("found shard that is not participating in the block (possibly duplicate)")

	WarnPrecedingShards = errors.New("prefix of multiple shards without announcement found")
)
