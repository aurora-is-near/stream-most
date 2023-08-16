package block_writer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aurora-is-near/stream-bridge/util"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_writer/monitoring"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Writer struct {
	options        *Options
	TipCached      *TipCached
	outputStream   stream.Interface
	lowHeightInRow uint64

	lastWritten      messages.BlockMessage
	closed           bool
	onCloseListeners []func(err error)
}

func (w *Writer) write(ctx context.Context, msg messages.BlockMessage) (*jetstream.PubAck, error) {
	if w.closed {
		return nil, ErrClosed
	}

	block := msg.GetBlock()
	if err := w.validate(block); err != nil {
		w.processError(err)
		return nil, err
	}

	headers := w.enrichHeaders(msg, msg.GetHeader())

	var lastError error
	var puback *jetstream.PubAck

	for attempts := w.options.MaxWriteAttempts; attempts >= 1; attempts-- {
		puback, lastError = w.outputStream.Write(
			context.Background(), // TODO
			&nats.Msg{
				Subject: "", // TODO!!!
				Header:  headers,
				Data:    msg.GetData(),
			},
			// TODO
		)
		if lastError == nil {
			w.lastWritten = msg
			if monitoring.LastWriteHeight != nil { // TODO: resolve normally
				monitoring.LastWriteHeight.Set(float64(block.GetHeight()))
			}

			if puback.Duplicate {
				logrus.Debugf("Duplicate message for a block with height %d", msg.GetHeight())
				return nil, ErrDuplicate
			}

			logrus.Debugf("Wrote a message for a block with height %d", msg.GetHeight())
			w.lowHeightInRow = 0
			return puback, nil
		}

		if attempts == 1 {
			break
		}

		if !util.CtxSleep(ctx, time.Millisecond*time.Duration(w.options.WriteRetryWaitMs)) {
			return nil, ErrCancelled
		}

		if ctx.Err() != nil {
			return nil, ErrCancelled
		}
	}
	return nil, lastError
}

func (w *Writer) WriteWithAck(ctx context.Context, msg messages.BlockMessage) (*jetstream.PubAck, error) {
	return w.write(ctx, msg)
}

func (w *Writer) Write(ctx context.Context, msg messages.BlockMessage) error {
	_, err := w.write(ctx, msg)
	return err
}

func (w *Writer) getTip() (messages.BlockMessage, error) {
	tip, err := w.TipCached.GetTip()
	if err != nil {
		return nil, err
	}

	if tip != nil {
		if monitoring.TipHeight != nil { // TODO: resolve normally
			monitoring.TipHeight.Set(float64(tip.GetHeight()))
		}
	}

	if w.lastWritten != nil && tip.GetHeight() < w.lastWritten.GetHeight() {
		return w.lastWritten, nil
	}
	return tip, err
}

func (w *Writer) validate(block blocks.Block) error {
	if w.options.BypassValidation {
		return nil
	}

	tip, err := w.getTip()
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			// Output stream is empty, so we can write any block
			return nil
		}
		return errors.Wrap(err, "cannot determine tip block for the output stream")
	}

	if tip != nil {
		if block.GetHeight() < tip.GetHeight() {
			return ErrLowHeight
		}
		if block.GetPrevHash() != tip.GetBlock().GetHash() && block.GetHash() != tip.GetHash() {
			return ErrHashMismatch
		}
	}
	return nil
}

func (w *Writer) enrichHeaders(message messages.BlockMessage, header nats.Header) nats.Header {
	var uniqueId string
	switch message.GetType() {
	case messages.Announcement:
		uniqueId = strconv.FormatUint(message.GetHeight(), 10)
	case messages.Shard:
		uniqueId = fmt.Sprintf("%d.%d", message.GetHeight(), message.GetShard().GetShardID())
	}

	if header == nil {
		header = make(nats.Header)
	}
	delete(header, "Nats-Expected-Stream")
	header.Add(nats.MsgIdHdr, uniqueId)
	return header
}

func (w *Writer) processError(err error) {
	switch err {
	case ErrLowHeight:
		w.lowHeightInRow++
		if w.lowHeightInRow >= w.options.LowHeightTolerance {
			w.gracefulShutdown(ErrLowHeight)
		}
	}
}

func (w *Writer) gracefulShutdown(err error) {
	w.closed = true
	for _, listener := range w.onCloseListeners {
		listener(err)
	}
}

func (w *Writer) OnClose(f func(error)) {
	w.onCloseListeners = append(w.onCloseListeners, f)
}

func NewWriter(options *Options, outputStream stream.Interface, peek TipPeeker) *Writer {
	return &Writer{
		options:      options,
		outputStream: outputStream,
		TipCached:    NewTipCached(options.TipTtl, peek),
	}
}
