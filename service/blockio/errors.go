package blockio

import "errors"

var (
	ErrTemporarilyUnavailable = errors.New("temporarily unavailable")
	ErrCompletelyUnavailable  = errors.New("completely unavailable")
	ErrCollision              = errors.New("collision")
	ErrCanceled               = errors.New("canceled")
	ErrWrongPredecessor       = errors.New("wrong predecessor")
	ErrRemovedPredecessor     = errors.New("removed predecessor")
	ErrRemovedPosition        = errors.New("removed position")
	ErrReseeked               = errors.New("reseeked")
)
