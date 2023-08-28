package bridge

import (
	"time"

	"github.com/aurora-is-near/stream-most/service2/blockwriter"
	"github.com/aurora-is-near/stream-most/service2/drivers/aligner"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

type Config struct {
	Format      string
	HeadersOnly bool

	Input            *stream.Options
	Reader           *reader.Options
	Output           *stream.Options
	TipCheckInterval time.Duration
	Aligner          *aligner.Config
	Writer           *blockwriter.Config
}
