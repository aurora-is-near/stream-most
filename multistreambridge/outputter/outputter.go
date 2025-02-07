package outputter

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/multistreambridge/headtracker"
	"github.com/aurora-is-near/stream-most/multistreambridge/jobrestarter"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

// Static assertion
var _ jobrestarter.Job = (*Outputter)(nil)

type Outputter struct {
	logger      *logrus.Entry
	cfg         *Config
	queue       chan *writeJob
	queueQuota  chan struct{}
	headTracker atomic.Pointer[headtracker.HeadTracker]
}

type writeJob struct {
	block  blocks.Block
	data   []byte
	done   chan struct{}
	status ResponseStatus
	head   *headtracker.HeadInfo
}

func NewOutputter(cfg *Config, numWriters uint) (*Outputter, error) {
	out := &Outputter{
		logger: logrus.
			WithField("component", "outputter").
			WithField("tag", cfg.LogTag),
		cfg:        cfg,
		queue:      make(chan *writeJob, numWriters),
		queueQuota: make(chan struct{}, numWriters),
	}
	return out, nil
}

func (out *Outputter) GetHeadInfo() *headtracker.HeadInfo {
	ht := out.headTracker.Load()
	if ht == nil {
		return nil
	}
	return ht.GetHeadInfo()
}

func (out *Outputter) GetStartHeight() uint64 {
	return out.cfg.StartHeight
}

func (out *Outputter) VerifyHeightAppend(height uint64) (status ResponseStatus, head *headtracker.HeadInfo) {
	head = out.GetHeadInfo()
	if head == nil {
		return OutputterUnavailable, head
	}

	if !head.HasBlock() {
		if height < out.cfg.StartHeight {
			return LowBlock, head
		}
		if height > out.cfg.StartHeight {
			return HighBlock, head
		}
		return OK, head
	}

	headBlock := head.BlockOrNone()

	if height <= headBlock.GetHeight() {
		return LowBlock, head
	}

	if formats.Active().GetFormat() == formats.AuroraV2 && height-headBlock.GetHeight() > 1 {
		return HighBlock, head
	}

	return Maybe, head
}

func (out *Outputter) VerifyBlockAppend(block blocks.Block) (status ResponseStatus, head *headtracker.HeadInfo) {
	status, head = out.VerifyHeightAppend(block.GetHeight())
	if status != Maybe {
		return status, head
	}

	h1 := head.BlockOrNone().GetHash()
	h2 := block.GetPrevHash()

	if formats.Active().GetFormat() == formats.AuroraV2 {
		h1 = strings.ToLower(h1)
		h2 = strings.ToLower(h2)
	}

	if h1 != h2 {
		return HashMismatch, head
	}

	return OK, head
}

func (out *Outputter) Run(ctx context.Context) {
	out.logger.Infof("connecting to stream...")
	sc, err := streamconnector.Connect(&streamconnector.Config{
		Nats: &transport.NATSConfig{
			ServerURL: strings.Join(out.cfg.NatsEndpoints, ","),
			Creds:     out.cfg.NatsCredsPath,
			Options:   transport.RecommendedNatsOptions(),
			LogTag:    out.cfg.LogTag,
		},
		Stream: &stream.Config{
			Name:        out.cfg.StreamName,
			RequestWait: time.Second * 10,
			WriteWait:   time.Second * 10,
			LogTag:      out.cfg.LogTag,
		},
	})
	if err != nil {
		out.logger.Errorf("can't connect to stream: %v", err)
		return
	}
	defer sc.Disconnect()

	headTracker, err := headtracker.StartHeadTracker(ctx, sc.Stream(), out.cfg.LogTag)
	if err != nil {
		out.logger.Errorf("unable to start head-tracker: %v", err)
		return
	}
	defer func() {
		headTracker.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("outputter is restarting"))
	}()

	out.headTracker.Store(headTracker)
	defer out.headTracker.Store(nil)

	for range cap(out.queueQuota) {
		out.queueQuota <- struct{}{}
	}
	defer out.drainQuota()

	for {
		select {
		case <-headTracker.Lifecycle().Ctx().Done():
			out.logger.Errorf("headtracker stopping: %v", headTracker.Lifecycle().StoppingReason())
			return
		case <-ctx.Done():
			out.logger.Infof("interrupted, stopping")
			return
		default:
		}

		select {
		case <-headTracker.Lifecycle().Ctx().Done():
			out.logger.Errorf("headtracker stopping: %v", headTracker.Lifecycle().StoppingReason())
			return
		case <-ctx.Done():
			out.logger.Infof("interrupted, stopping")
			return
		case j := <-out.queue:
			out.queueQuota <- struct{}{}
			if err := out.handleWrite(ctx, sc.Stream(), headTracker, j); err != nil {
				out.logger.Errorf("unable to write: %v", err)
				return
			}
		}
	}
}

func (out *Outputter) drainQuota() {
	for range cap(out.queueQuota) {
		select {
		case <-out.queueQuota:
		case j := <-out.queue:
			j.status = OutputterUnavailable
			close(j.done)
		}
	}
}

func (out *Outputter) handleWrite(ctx context.Context, s *stream.Stream, ht *headtracker.HeadTracker, j *writeJob) error {
	defer close(j.done)

	j.status, j.head = out.VerifyBlockAppend(j.block)
	if j.status != OK {
		return nil
	}

	ack, err := s.Write(
		ctx,
		&nats.Msg{
			Subject: out.cfg.Subject,
			Data:    j.data,
		},
		jetstream.WithExpectLastSequence(j.head.Sequence()),
		jetstream.WithMsgID(blocks.ConstructMsgID(j.block)),
		jetstream.WithRetryAttempts(3),
		jetstream.WithRetryWait(time.Second),
	)

	if err != nil {
		if isWrongExpectedLastSequence(err) {
			return out.handleWriteRace(ctx, ht, j,
				fmt.Errorf(
					"wrong-expected-last-seq (height=%d, expectedLastSeq=%d): %w",
					j.block.GetHeight(), j.head.Sequence(), err,
				),
			)
		}

		j.status = OutputterUnavailable
		return fmt.Errorf(
			"unable to write block with height=%d to jetstream at seq=%d, got error: %w",
			j.block.GetHeight(), j.head.Sequence()+1, err,
		)
	}

	if ack.Duplicate {
		return out.handleWriteRace(ctx, ht, j,
			fmt.Errorf(
				"duplicate height %d (trying to write on seq=%d)",
				j.block.GetHeight(), j.head.Sequence()+1,
			),
		)
	}

	ht.UpdateHeadInfo(ack.Sequence, j.block)
	return nil
}

func (out *Outputter) handleWriteRace(ctx context.Context, ht *headtracker.HeadTracker, j *writeJob, race error) error {
	if waitErr := out.waitGreaterHead(ctx, ht, j.head); waitErr != nil {
		j.status = OutputterUnavailable
		return fmt.Errorf(
			"unable to write block with height=%d to jetstream at seq=%d: got race (%w),"+
				"but wasn't able to wait for greater head: %w",
			j.block.GetHeight(), j.head.Sequence()+1, race, waitErr,
		)
	}

	oldHead := j.head
	j.status, j.head = out.VerifyBlockAppend(j.block)
	if j.status != OK {
		return nil
	}

	j.status = OutputterUnavailable
	return fmt.Errorf(
		"getting non-sensical results: block with height=%d can be appended after both seq=%d and seq=%d (caused by race: %w)",
		j.block.GetHeight(), oldHead.Sequence(), j.head.Sequence(), race,
	)
}

func (out *Outputter) waitGreaterHead(
	ctx context.Context, ht *headtracker.HeadTracker, curHead *headtracker.HeadInfo,
) error {

	if newHead := ht.GetHeadInfo(); newHead == nil || newHead.Sequence() > curHead.Sequence() {
		return nil
	}

	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()

	start := time.Now()
	for {
		if time.Since(start) > time.Second*10 {
			return fmt.Errorf("no new head coming from head-tracker for 10 seconds")
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted while waiting for newer head from head-tracker")
		default:
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted while waiting for newer head from head-tracker")
		case <-ticker.C:
			if newHead := ht.GetHeadInfo(); newHead == nil || newHead.Sequence() > curHead.Sequence() {
				return nil
			}
		}
	}
}

func (out *Outputter) TryWrite(ctx context.Context, block blocks.Block, data []byte) (status ResponseStatus, head *headtracker.HeadInfo) {
	status, head = out.VerifyBlockAppend(block)
	if status != OK {
		return status, head
	}

	select {
	case <-out.queueQuota:
	default:
		return OutputterUnavailable, nil
	}

	j := &writeJob{
		block: block,
		data:  data,
		done:  make(chan struct{}),
	}

	out.queue <- j

	select {
	case <-ctx.Done():
		return Interrupted, nil
	case <-j.done:
	}

	return j.status, j.head
}
