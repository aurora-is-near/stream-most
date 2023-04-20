package filesystem

import (
	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/pkg/errors"
)

type Storage struct {
	chunks chunks.ChunksInterface
}

func (s *Storage) init() error {
	if err := s.chunks.Open(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) Write(message messages.AbstractNatsMessage) error {
	return nil
}

func NewStorage(opts *Options) *Storage {
	s := &Storage{
		chunks: &chunks.Chunks{
			Dir:             opts.Dir,
			ChunkNamePrefix: opts.ChunkNamePrefix + "_",
		},
	}

	err := s.init()
	if err != nil {
		panic(errors.Wrap(err, "can't init filesystem storage: "))
	}

	return s
}
