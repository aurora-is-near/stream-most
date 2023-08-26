package blockwriter

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrInvalidCfg = fmt.Errorf("invalid configuration")

type Writer struct {
	config *Config
	output stream.Interface

	subjectPattern string
	noExpectOn     blocks.Block
	noExpectAfter  blocks.Block
}

func NewWriter(ctx context.Context, config *Config, output stream.Interface) (*Writer, error) {
	w := &Writer{
		config: config,
		output: output,
	}

	if config.SubjectPattern != "" {
		w.subjectPattern = config.SubjectPattern
	} else {
		subjects, err := output.GetConfigSubjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get output config subjects: %w", err)
		}
		if len(subjects) != 1 {
			return nil, fmt.Errorf(
				"unable to recognize subject automatically, expected just one subject, got [%s] (%w)",
				strings.Join(subjects, ", "),
				ErrInvalidCfg,
			)
		}
		w.subjectPattern = subjects[0]
	}

	var err error
	if config.DisableExpectHeadersForMsgID != "" {
		w.noExpectOn, err = formats.Active().ParseMsgID(config.DisableExpectHeadersForMsgID)
		if err != nil {
			return nil, fmt.Errorf("unable to parse DisableExpectHeadersForMsgID: %s (%w)", err.Error(), ErrInvalidCfg)
		}
	}
	if config.DisableExpectHeadersForNextAfterMsgID != "" {
		w.noExpectAfter, err = formats.Active().ParseMsgID(config.DisableExpectHeadersForNextAfterMsgID)
		if err != nil {
			return nil, fmt.Errorf("unable to parse DisableExpectHeadersForNextAfterMsgID: %s (%w)", err.Error(), ErrInvalidCfg)
		}
	}

	return w, nil
}

func (w *Writer) Write(ctx context.Context, tip, msg *messages.BlockMessage, lastSeq uint64) (*jetstream.PubAck, error) {
	wMsg := &nats.Msg{
		Header: make(nats.Header),
		Data:   msg.Msg.GetData(),
	}

	switch msg.Block.GetBlockType() {
	case blocks.Announcement:
		wMsg.Subject = strings.ReplaceAll(w.subjectPattern, "*", "header")
	case blocks.Shard:
		wMsg.Subject = strings.ReplaceAll(w.subjectPattern, "*", strconv.FormatUint(msg.Block.GetShardID(), 10))
	default:
		wMsg.Subject = strings.ReplaceAll(w.subjectPattern, "*", "unknown")
	}

	if w.config.PreserveCustomHeaders {
		for k, v := range msg.Msg.GetHeader() {
			if _, ok := serviceHeaders[k]; !ok {
				wMsg.Header[k] = v
			}
		}
	}

	return w.output.Write(ctx, wMsg, w.ConstructPublishOpts(tip, msg, lastSeq)...)
}

func (w *Writer) ConstructPublishOpts(tip, msg *messages.BlockMessage, lastSeq uint64) []jetstream.PublishOpt {
	opts := []jetstream.PublishOpt{
		jetstream.WithMsgID(blocks.ConstructMsgID(msg.Block)),
		jetstream.WithRetryWait(w.config.RetryWait),
		jetstream.WithRetryAttempts(w.config.RetryAttempts),
	}

	if w.config.DisableExpectHeadersCompletely {
		return opts
	}
	if w.config.DisableExpectHeadersForSeq == lastSeq+1 {
		return opts
	}
	if w.noExpectOn != nil && blocks.Equal(msg.Block, w.noExpectOn) {
		return opts
	}
	if w.noExpectAfter != nil && tip != nil && blocks.Equal(tip.Block, w.noExpectAfter) {
		return opts
	}

	if tip != nil {
		opts = append(opts, jetstream.WithExpectLastMsgID(blocks.ConstructMsgID(tip.Block)))
	}
	opts = append(opts, jetstream.WithExpectLastSequence(lastSeq))

	return opts
}
