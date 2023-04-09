package block_writer

import "errors"

var (
	ErrClosed       = errors.New("closed")
	ErrDuplicate    = errors.New("duplicate")
	ErrLowHeight    = errors.New("low height")
	ErrHashMismatch = errors.New("hash mismatch")
	ErrCancelled    = errors.New("cancelled")
)
