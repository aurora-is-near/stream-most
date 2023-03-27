package block_writer

import "errors"

var (
	ErrLowHeight    = errors.New("low height")
	ErrHashMismatch = errors.New("hash mismatch")
	ErrCancelled    = errors.New("cancelled")
)
