package blockio

import "errors"

var (
	ErrCantDecode             = errors.New("cant decode")
	ErrTemporarilyUnavailable = errors.New("temporarily unavailable")
	ErrCompletelyUnavailable  = errors.New("completely unavailable")
	ErrCollision              = errors.New("collision")
)
