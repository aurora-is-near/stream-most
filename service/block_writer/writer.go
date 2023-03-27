package block_writer

import (
	"context"
	"fmt"
	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"strconv"
	"time"
)

type Writer struct {
	options      *Options
	TipCached    *TipCached
	outputStream stream.Interface

	lastWritten messages.AbstractNatsMessage
}

func (w *Writer) Write(ctx context.Context, msg messages.AbstractNatsMessage) error {
	block := msg.GetBlock()
	if err := w.validate(block); err != nil {
		return err
	}

	headers := w.enrichHeaders(msg, msg.GetMsg().Header)

	var lastError error
	for attempts := w.options.MaxWriteAttempts; attempts >= 1; attempts-- {
		_, lastError = w.outputStream.Write(
			msg.GetMsg().Data,
			headers,
			nats.AckWait(1*time.Second),
		)
		if lastError == nil {
			w.lastWritten = msg
			return nil
		}

		if attempts == 1 {
			break
		}

		if !util.CtxSleep(ctx, time.Millisecond*time.Duration(w.options.WriteRetryWaitMs)) {
			return ErrCancelled
		}

		if ctx.Err() != nil {
			return ErrCancelled
		}
	}
	return nil
}

func (w *Writer) getTip() (messages.AbstractNatsMessage, error) {
	tip, err := w.TipCached.GetTip()
	if w.lastWritten != nil && tip.GetBlock().Height < w.lastWritten.GetBlock().Height {
		return w.lastWritten, nil
	}
	return tip, err
}

func (w *Writer) validate(block *blocks.AbstractBlock) error {
	tip, err := w.getTip()
	if err != nil {
		return err
	}

	if tip != nil {
		if block.Height < tip.GetBlock().Height {
			return ErrLowHeight
		}
		if block.PrevHash != tip.GetBlock().Hash && block.Hash != tip.GetBlock().Hash {
			return ErrHashMismatch
		}
	}
	return nil
}

func (w *Writer) enrichHeaders(message messages.AbstractNatsMessage, header nats.Header) nats.Header {
	var uniqueId string
	if message.IsAnnouncement() {
		uniqueId = strconv.FormatUint(message.GetBlock().Height, 10)
	}
	if message.IsShard() {
		uniqueId = fmt.Sprintf("%d:%d", message.GetBlock().Height, message.GetShard().ShardID)
	}

	header.Add(nats.MsgIdHdr, uniqueId)
	return header
}

func NewWriter(options *Options, outputStream stream.Interface, peek TipPeeker) *Writer {
	return &Writer{
		options:      options,
		outputStream: outputStream,
		TipCached:    NewTipCached(options.TipTtl, peek),
	}
}
