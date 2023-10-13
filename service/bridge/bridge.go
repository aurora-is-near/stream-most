package bridge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/verifier"
	"github.com/aurora-is-near/stream-most/support/tolerance"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var (
	ErrStopped                  = fmt.Errorf("stopped externally")
	ErrTerminalConditionReached = fmt.Errorf("terminal condition reached")
	ErrDesync                   = fmt.Errorf("desync")
)

type Bridge struct {
	logger *logrus.Entry

	config              *Config
	verifier            verifier.Verifier
	headersOnlyVerifier verifier.Verifier
	noFilterVerifier    verifier.Verifier
	input               blockio.Input
	output              blockio.Output

	reorderBuffer     []*messages.BlockMessage
	reorderBufferSwap []*messages.BlockMessage

	corruptedBlocksTolerance *tolerance.Tolerance
	lowBlocksTolerance       *tolerance.Tolerance
	highBlocksTolerance      *tolerance.Tolerance
	wrongBlocksTolerance     *tolerance.Tolerance // corrupted + high
	noWriteTolerance         *tolerance.Tolerance // corrupted + low + high

	ctx      context.Context
	cancel   func()
	finished chan struct{}
	finalErr error
}

func Start(cfg *Config, verifier verifier.Verifier, input blockio.Input, output blockio.Output) (*Bridge, error) {
	b := &Bridge{
		logger: logrus.WithField("component", "bridge"),

		config:              cfg,
		verifier:            verifier,
		headersOnlyVerifier: verifier.WithHeadersOnly(),
		noFilterVerifier:    verifier.WithNoShardFilter(),
		input:               input,
		output:              output,

		reorderBuffer:     make([]*messages.BlockMessage, 0),
		reorderBufferSwap: make([]*messages.BlockMessage, 0),

		corruptedBlocksTolerance: tolerance.NewTolerance(cfg.CorruptedBlocksTolerance),
		lowBlocksTolerance:       tolerance.NewTolerance(cfg.LowBlocksTolerance),
		highBlocksTolerance:      tolerance.NewTolerance(cfg.HighBlocksTolerance),
		wrongBlocksTolerance:     tolerance.NewTolerance(cfg.WrongBlocksTolerance),
		noWriteTolerance:         tolerance.NewTolerance(cfg.NoWriteTolerance),

		finished: make(chan struct{}),
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	go b.run()

	return b, nil
}

func (b *Bridge) Stop(wait bool) {
	b.cancel()
	if wait {
		<-b.finished
	}
}

func (b *Bridge) Finished() <-chan struct{} {
	return b.finished
}

func (b *Bridge) Error() error {
	select {
	case <-b.finished:
		return b.finalErr
	default:
		return nil
	}
}

func (b *Bridge) run() {
	defer close(b.finished)

	b.finalErr = b.runLoop()

	switch {
	case errors.Is(b.finalErr, ErrTerminalConditionReached):
		b.logger.Infof("Finished because terminal condition was reached: %v", b.finalErr)
	case errors.Is(b.finalErr, ErrStopped):
		b.logger.Warnf("Stopped externally")
	default:
		b.logger.Errorf("Finished with error: %v", b.finalErr)
	}
}

func (b *Bridge) runLoop() error {
	for i := 0; b.ctx.Err() == nil; i++ {
		b.logger.Infof("Running bridging attempt #%d...", i+1)

		err := b.seekAndSync()
		if !errors.Is(err, ErrDesync) {
			return err
		}

		b.logger.Errorf("Got syncing problem: %v", err)
		if b.config.MaxReseeks >= 0 && i >= b.config.MaxReseeks {
			return fmt.Errorf("max reseeks (%d) exceeded: %w", b.config.MaxReseeks, err)
		}

		b.logger.Infof("Sleeping for %s before next reseek...", b.config.ReseekDelay.String())
		if !util.CtxSleep(b.ctx, b.config.ReseekDelay) {
			return ErrStopped
		}
	}
	return ErrStopped
}

func (b *Bridge) seekAndSync() error {
	defer b.resetReorderBuffer()

	b.tuneTolerance(false, 0)

	session, err := b.seek()
	if err != nil {
		return err
	}

	bufferUnloadTicker := time.NewTicker(time.Second)
	defer bufferUnloadTicker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return ErrStopped
		default:
		}

		var inputMsg blockio.Msg
		var inputOpen bool
		gotInput := false

		select {
		case inputMsg, inputOpen = <-session.Msgs():
			gotInput = true
		default:
			select {
			case <-b.ctx.Done():
				return ErrStopped
			case inputMsg, inputOpen = <-session.Msgs():
				gotInput = true
			case <-bufferUnloadTicker.C:
			}
		}

		if err := b.unloadBuffer(); err != nil {
			return err
		}

		if gotInput {
			if !inputOpen {
				if session.Error() == nil {
					return fmt.Errorf("end of input (%w)", ErrTerminalConditionReached)
				}
				return fmt.Errorf("input closed: %w", session.Error())
			}
			if err := b.handleInputMsg(inputMsg); err != nil {
				return err
			}

			if err := b.unloadBuffer(); err != nil {
				return err
			}
		}
	}
}

func (b *Bridge) unloadBuffer() error {
	if len(b.reorderBuffer) == 0 {
		return nil
	}

	sort.Slice(b.reorderBuffer, func(i, j int) bool {
		if blocks.Less(b.reorderBuffer[i].Block, b.reorderBuffer[j].Block) {
			return true
		}
		if blocks.Equal(b.reorderBuffer[i].Block, b.reorderBuffer[j].Block) {
			return b.reorderBuffer[i].Msg.GetSequence() < b.reorderBuffer[j].Msg.GetSequence()
		}
		return false
	})

	if len(b.reorderBuffer) > int(b.config.ReorderBufferSize) {
		b.reorderBuffer = b.reorderBuffer[:b.config.ReorderBufferSize]
	}

	b.reorderBufferSwap = b.reorderBufferSwap[:0]
	lastHandledPos := -1

	defer func() {
		for i := lastHandledPos + 1; i < len(b.reorderBuffer); i++ {
			b.reorderBufferSwap = append(b.reorderBufferSwap, b.reorderBuffer[i])
		}
		b.reorderBuffer, b.reorderBufferSwap = b.reorderBufferSwap, b.reorderBuffer[:0]
	}()

	for i, msg := range b.reorderBuffer {
		lastHandledPos = i - 1

		select {
		case <-b.ctx.Done():
			return ErrStopped
		default:
		}

		outputTip, _, err := b.getOutputTip()
		if err != nil {
			return err
		}
		if outputTip == nil {
			continue
		}

		if err := b.verifier.CanAppend(outputTip, msg); err != nil {
			if errors.Is(err, verifier.ErrMayBeRelevantLater) {
				b.reorderBufferSwap = append(b.reorderBufferSwap, msg)
			}
			continue
		}

		if b.config.EndBlock != nil && !blocks.Less(msg.Block, b.config.EndBlock) {
			return fmt.Errorf(
				"%w: next block to write (%s) >= end block (%s)",
				ErrTerminalConditionReached,
				blocks.ConstructMsgID(msg.Block),
				blocks.ConstructMsgID(b.config.EndBlock),
			)
		}

		if err := b.waitWrite(outputTip.Msg.GetSequence(), outputTip.Msg.GetHeader().Get(jetstream.MsgIDHeader), msg); err != nil {
			return err
		}

		if b.config.LastBlock != nil && !blocks.Less(msg.Block, b.config.LastBlock) {
			return fmt.Errorf(
				"%w: last written block (%s) >= last block (%s)",
				ErrTerminalConditionReached,
				blocks.ConstructMsgID(msg.Block),
				blocks.ConstructMsgID(b.config.LastBlock),
			)
		}
	}

	lastHandledPos = len(b.reorderBuffer) - 1

	return nil
}

func (b *Bridge) handleInputMsg(msg blockio.Msg) error {
	outputTip, outputTipSeq, err := b.getOutputTip()
	if err != nil {
		return err
	}

	msgBlock, err := msg.GetDecoded(b.ctx)
	if err != nil {
		if b.ctx.Err() != nil && errors.Is(err, b.ctx.Err()) {
			return ErrStopped
		}
		return b.tuneTolerance(true, 0)
	}

	if outputTip == nil {
		return b.handleInputMsgWithNoTip(msgBlock, outputTipSeq)
	}

	return b.tryAppend(outputTip, msgBlock, outputTipSeq, outputTip.Msg.GetHeader().Get(jetstream.MsgIDHeader))
}

func (b *Bridge) handleInputMsgWithNoTip(msgBlock *messages.BlockMessage, outputTipSeq uint64) error {
	if b.config.AnchorInputSeq == 0 {
		return fmt.Errorf("no output tip (seq=%d) and no output sequence anchor provided, unable to bridge", outputTipSeq)
	}
	if b.config.AnchorOutputSeq != outputTipSeq+1 {
		return fmt.Errorf(
			"no output tip (seq=%d) and mismatching output sequence anchor is provided (%d, but must be tip+1), unable to bridge",
			outputTipSeq,
			b.config.AnchorOutputSeq,
		)
	}
	if b.config.AnchorBlock != nil {
		if blocks.Equal(msgBlock.Block, b.config.AnchorBlock) {
			b.tuneTolerance(false, 0)
			return b.waitWrite(outputTipSeq, "", msgBlock)
		}
		if blocks.Less(msgBlock.Block, b.config.AnchorBlock) {
			return b.tuneTolerance(false, -1)
		}
		b.addToBuffer(msgBlock)
		return b.tuneTolerance(false, 1)
	}
	if b.config.PreAnchorBlock != nil {
		return b.tryAppend(&messages.BlockMessage{Block: b.config.PreAnchorBlock}, msgBlock, outputTipSeq, "")
	}
	if b.config.AnchorInputSeq > 0 {
		switch {
		case msgBlock.Msg.GetSequence() < b.config.AnchorInputSeq:
			return b.tuneTolerance(false, -1)
		case msgBlock.Msg.GetSequence() > b.config.AnchorInputSeq:
			return fmt.Errorf(
				"%w: no output tip (seq=%d) and current block seq (%d) is greater than provided input sequence anchor (%d), unable to bridge",
				ErrDesync,
				outputTipSeq,
				msgBlock.Msg.GetSequence(),
				b.config.AnchorInputSeq,
			)
		default:
			b.tuneTolerance(false, 0)
			return b.waitWrite(outputTipSeq, "", msgBlock)
		}
	}
	return fmt.Errorf("no output tip (seq=%d) and no input anchors provided, unable to seek", outputTipSeq)
}

func (b *Bridge) tryAppend(tip, msg *messages.BlockMessage, tipSeq uint64, tipMsgID string) error {
	err := b.verifier.CanAppend(tip, msg)
	switch {

	case err == nil:
		b.tuneTolerance(false, 0)
		if b.config.EndBlock != nil && !blocks.Less(msg.Block, b.config.EndBlock) {
			return fmt.Errorf(
				"%w: next block to write (%s) >= end block (%s)",
				ErrTerminalConditionReached,
				blocks.ConstructMsgID(msg.Block),
				blocks.ConstructMsgID(b.config.EndBlock),
			)
		}
		if err := b.waitWrite(tipSeq, tipMsgID, msg); err != nil {
			return err
		}
		if b.config.LastBlock != nil && !blocks.Less(msg.Block, b.config.LastBlock) {
			return fmt.Errorf(
				"%w: last written block (%s) >= last block (%s)",
				ErrTerminalConditionReached,
				blocks.ConstructMsgID(msg.Block),
				blocks.ConstructMsgID(b.config.LastBlock),
			)
		}
		return nil

	case errors.Is(err, verifier.ErrCompletelyIrrelevant):
		if errors.Is(err, verifier.ErrFilteredShard) {
			err2 := b.headersOnlyVerifier.CanAppend(tip, msg)
			switch {
			case err2 == nil:
				return b.tuneTolerance(false, 0)
			case errors.Is(err2, verifier.ErrAlreadyIrrelevant):
				return b.tuneTolerance(false, -1)
			case errors.Is(err2, verifier.ErrMayBeRelevantLater):
				return b.tuneTolerance(false, 1)
			}
		}
		return b.tuneTolerance(true, 0)

	case errors.Is(err, verifier.ErrAlreadyIrrelevant):
		return b.tuneTolerance(false, -1)

	case errors.Is(err, verifier.ErrMayBeRelevantLater):
		b.addToBuffer(msg)
		return b.tuneTolerance(false, 1)

	default:
		return fmt.Errorf("got unknown verifier error: %w", err)
	}
}

func (b *Bridge) addToBuffer(msg *messages.BlockMessage) {
	if b.config.ReorderBufferSize > 0 {
		b.reorderBuffer = append(b.reorderBuffer, msg)
	}
}

func (b *Bridge) seek() (blockio.InputSession, error) {
	outputTip, outputTipSeq, err := b.getOutputTip()
	if err != nil {
		return nil, err
	}

	if outputTip != nil {
		b.logger.Infof("Seeking input by output tip: %s", blocks.ConstructMsgID(outputTip.Block))
		return b.input.SeekBlock(outputTip.Block, b.config.InputStartSeq, b.config.InputEndSeq, true), nil
	}

	b.logger.Warnf("No output tip (seq=%d), will try to use anchors", outputTipSeq)

	if b.config.AnchorOutputSeq == 0 {
		return nil, fmt.Errorf("no output tip (seq=%d) and no output sequence anchor provided, unable to seek", outputTipSeq)
	}

	if b.config.AnchorOutputSeq != outputTipSeq+1 {
		return nil, fmt.Errorf(
			"no output tip (seq=%d) and mismatching output sequence anchor is provided (%d, but must be tip+1), unable to seek",
			outputTipSeq,
			b.config.AnchorOutputSeq,
		)
	}

	b.logger.Infof("Output sequence anchor matches next output seq (%d)", outputTipSeq+1)

	if b.config.AnchorBlock != nil {
		b.logger.Infof("Anchor block is provided (%s), will use it for seeking", blocks.ConstructMsgID(b.config.AnchorBlock))
		return b.input.SeekBlock(b.config.AnchorBlock, b.config.InputStartSeq, b.config.InputEndSeq, false), nil
	}

	if b.config.PreAnchorBlock != nil {
		b.logger.Infof("Pre-anchor block is provided (%s), will use it for seeking", blocks.ConstructMsgID(b.config.PreAnchorBlock))
		return b.input.SeekBlock(b.config.PreAnchorBlock, b.config.InputStartSeq, b.config.InputEndSeq, true), nil
	}

	if b.config.AnchorInputSeq > 0 {
		b.logger.Infof("Input sequence anchor provided (%d), will use it for seeking", b.config.AnchorInputSeq)
		return b.input.SeekSeq(b.config.AnchorInputSeq, b.config.InputStartSeq, b.config.InputEndSeq), nil
	}

	return nil, fmt.Errorf("no output tip (seq=%d) and no input anchors provided, unable to seek", outputTipSeq)
}

func (b *Bridge) getOutputTip() (*messages.BlockMessage, uint64, error) {
	outputTip, outputTipSeq, err := b.waitLastKnownMsg(b.output)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get output tip: %w", err)
	}
	if err := b.checkTerminalCondition(outputTip); err != nil {
		return nil, 0, err
	}

	lastKnownDeletedSeq, err := b.waitLastKnownDeletedSeq(b.output)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get last known deleted output seq: %w", err)
	}
	if lastKnownDeletedSeq >= outputTipSeq {
		return nil, lastKnownDeletedSeq, nil
	}

	return outputTip, outputTipSeq, nil
}

func (b *Bridge) waitLastKnownMsg(s blockio.StateProvider) (*messages.BlockMessage, uint64, error) {
	for {
		m, mseq, err := s.LastKnownMessage()
		if err == nil {
			if m == nil {
				return nil, mseq, nil
			}
			mDecoded, err := m.GetDecoded(b.ctx)
			if err != nil {
				if b.ctx.Err() != nil && errors.Is(err, b.ctx.Err()) {
					return nil, 0, ErrStopped
				}
				return nil, 0, fmt.Errorf("unable to decode last known block (seq=%d): %w", mseq, err)
			}
			return mDecoded, mseq, nil
		}
		if !errors.Is(err, blockio.ErrTemporarilyUnavailable) {
			return nil, 0, err
		}
		if !util.CtxSleep(b.ctx, time.Second/10) {
			return nil, 0, ErrStopped
		}
	}
}

func (b *Bridge) waitLastKnownDeletedSeq(s blockio.StateProvider) (uint64, error) {
	for {
		seq, err := s.LastKnownDeletedSeq()
		if err == nil {
			return seq, nil
		}
		if !errors.Is(err, blockio.ErrTemporarilyUnavailable) {
			return 0, err
		}
		if !util.CtxSleep(b.ctx, time.Second/10) {
			return 0, ErrStopped
		}
	}
}

func (b *Bridge) waitWrite(predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error {
	for {
		err := b.output.ProtectedWrite(b.ctx, predecessorSeq, predecessorMsgID, msg)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, blockio.ErrTemporarilyUnavailable):
			if !util.CtxSleep(b.ctx, time.Second/10) {
				return ErrStopped
			}
		case errors.Is(err, blockio.ErrCanceled):
			return ErrStopped
		case errors.Is(err, blockio.ErrCollision):
			fallthrough
		case errors.Is(err, blockio.ErrWrongPredecessor):
			fallthrough
		case errors.Is(err, blockio.ErrRemovedPredecessor):
			fallthrough
		case errors.Is(err, blockio.ErrRemovedPosition):
			return fmt.Errorf(
				"%w: unable to write block '%s' on seq %d (predecessorMsgID='%s'): %w",
				ErrDesync,
				blocks.ConstructMsgID(msg.Block),
				predecessorSeq+1,
				predecessorMsgID,
				err,
			)
		default:
			return fmt.Errorf(
				"unable to write block '%s' on seq %d (predecessorMsgID='%s'): %w",
				blocks.ConstructMsgID(msg.Block),
				predecessorSeq+1,
				predecessorMsgID,
				err,
			)
		}
	}
}

func (b *Bridge) resetReorderBuffer() {
	b.reorderBuffer = b.reorderBuffer[:0]
	b.reorderBufferSwap = b.reorderBufferSwap[:0]
}

func (b *Bridge) checkTerminalCondition(outputTip *messages.BlockMessage) error {
	if outputTip == nil {
		return nil
	}
	if b.config.LastBlock != nil && !blocks.Less(outputTip.Block, b.config.LastBlock) {
		return fmt.Errorf(
			"%w: output tip (%s) >= last block (%s)",
			ErrTerminalConditionReached,
			blocks.ConstructMsgID(outputTip.Block),
			blocks.ConstructMsgID(b.config.LastBlock),
		)
	}
	if b.config.EndBlock != nil && !blocks.Less(outputTip.Block, b.config.EndBlock) {
		return fmt.Errorf(
			"%w: output tip (%s) >= end block (%s)",
			ErrTerminalConditionReached,
			blocks.ConstructMsgID(outputTip.Block),
			blocks.ConstructMsgID(b.config.EndBlock),
		)
	}
	return nil
}

func (b *Bridge) tuneTolerance(corrupted bool, tipCmp int) error {
	if corrupted {
		if !b.corruptedBlocksTolerance.Tolerate(1) {
			return fmt.Errorf("%w: corrupted blocks tolerance (%d) exceeded", ErrDesync, b.config.CorruptedBlocksTolerance)
		}
		if !b.wrongBlocksTolerance.Tolerate(1) {
			return fmt.Errorf("%w: wrong blocks tolerance (%d) exceeded", ErrDesync, b.config.WrongBlocksTolerance)
		}
		if !b.noWriteTolerance.Tolerate(1) {
			return fmt.Errorf("%w: no-write tolerance (%d) exceeded", ErrDesync, b.config.NoWriteTolerance)
		}
		return nil
	}
	b.corruptedBlocksTolerance.Reset()

	switch {

	case tipCmp < 0:
		b.highBlocksTolerance.Reset()
		b.wrongBlocksTolerance.Reset()
		if !b.lowBlocksTolerance.Tolerate(1) {
			return fmt.Errorf("%w: low blocks tolerance (%d) exceeded", ErrDesync, b.config.LowBlocksTolerance)
		}
		if !b.noWriteTolerance.Tolerate(1) {
			return fmt.Errorf("%w: no-write tolerance (%d) exceeded", ErrDesync, b.config.NoWriteTolerance)
		}

	case tipCmp > 0:
		b.lowBlocksTolerance.Reset()
		if !b.highBlocksTolerance.Tolerate(1) {
			return fmt.Errorf("%w: high blocks tolerance (%d) exceeded", ErrDesync, b.config.HighBlocksTolerance)
		}
		if !b.wrongBlocksTolerance.Tolerate(1) {
			return fmt.Errorf("%w: wrong blocks tolerance (%d) exceeded", ErrDesync, b.config.WrongBlocksTolerance)
		}
		if !b.noWriteTolerance.Tolerate(1) {
			return fmt.Errorf("%w: no-write tolerance (%d) exceeded", ErrDesync, b.config.NoWriteTolerance)
		}

	default:
		b.lowBlocksTolerance.Reset()
		b.highBlocksTolerance.Reset()
		b.wrongBlocksTolerance.Reset()
		b.noWriteTolerance.Reset()
	}

	return nil
}
