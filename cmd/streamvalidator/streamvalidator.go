package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/formats/headers"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/multistreambridge/blockreader"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	natsUrl         = flag.String("nats", "", "nats endpoint(s) (comma separated)")
	natsCredsPath   = flag.String("creds", "", "path to creds")
	format          = flag.String("format", "", "blocks format ('aurora' or 'near')")
	streamName      = flag.String("stream", "", "stream name")
	stateFile       = flag.String("state", "", "path to state file")
	requireStartSeq = flag.Uint64("require-start-seq", 0, "required start sequence (0 means any)")
)

func main() {
	flag.Parse()

	if *natsUrl == "" {
		log.Fatalf("-nats flag must be provided")
	}
	if *format == "" {
		log.Fatalf("-format flag must be provided")
	}
	if *streamName == "" {
		log.Fatalf("-stream flag must be provided")
	}
	if *stateFile == "" {
		log.Fatalf("-state flag must be provided")
	}

	switch strings.ToLower(*format) {
	case "aurora":
		formats.UseFormat(formats.AuroraV2)
	case "near":
		formats.UseFormat(formats.NearV2)
	default:
		log.Fatalf("unknown blocks format '%s'", *format)
	}

	sc, err := streamconnector.Connect(&streamconnector.Config{
		Nats: &transport.NATSConfig{
			ServerURL: *natsUrl,
			Creds:     *natsCredsPath,
			Options:   transport.RecommendedNatsOptions(),
		},
		Stream: &stream.Config{
			Name:        *streamName,
			RequestWait: time.Second * 10,
			WriteWait:   time.Second * 10,
		},
	})
	if err != nil {
		log.Fatalf("unable to connect to stream: %v", err)
	}
	defer sc.Disconnect()

	lastCheckedSeq, err := loadState()
	if err != nil {
		log.Fatalf("unable to read state file: %v", err)
	}

	startSeq := max(lastCheckedSeq, *requireStartSeq)
	strictStart := (startSeq > 0)

	var prevBlock blocks.Block
	lastStateSaveTime := time.Now()
	lastLogTime := time.Now()
	msgsHandledSinceLastLog := uint64(0)

	br, err := blockreader.StartBlockReader(context.Background(), &blockreader.Options{
		Stream:      sc.Stream(),
		StartSeq:    startSeq,
		StrictStart: strictStart,
		BlockCb: func(ctx context.Context, blockMsg *messages.BlockMessage) (_err error) {
			hdr := blockMsg.Msg.GetHeader().Get(jetstream.MsgIDHeader)
			if len(hdr) == 0 {
				return fmt.Errorf("no msgid header on seq=%d", blockMsg.Msg.GetSequence())
			}
			msgid, err := headers.ParseMsgID(hdr)
			if err != nil {
				return fmt.Errorf("unable to parse msgid header ('%s') on seq=%d: %w", hdr, blockMsg.Msg.GetSequence(), err)
			}
			if msgid.GetHeight() != blockMsg.Block.GetHeight() {
				return fmt.Errorf(
					"msgid height (%d) doesn't match real block height (%d) on seq=%d",
					msgid.GetHeight(), blockMsg.Block.GetHeight(), blockMsg.Msg.GetSequence(),
				)
			}
			if prevBlock != nil {
				if err := validate(prevBlock, blockMsg.Block); err != nil {
					return fmt.Errorf("validation error on seq=%d: %w", blockMsg.Msg.GetSequence(), err)
				}
			}
			prevBlock = blockMsg.Block
			msgsHandledSinceLastLog++

			if tDiff := time.Since(lastLogTime); tDiff > time.Second*3 {
				log.Printf(
					"seq=%d, height=%d, speed=%0.2fmsgs/sec",
					blockMsg.Msg.GetSequence(),
					blockMsg.Block.GetHeight(),
					float64(msgsHandledSinceLastLog)/tDiff.Seconds(),
				)
				lastLogTime = time.Now()
				msgsHandledSinceLastLog = 0
			}

			if time.Since(lastStateSaveTime) > time.Second*2 {
				if err := saveState(blockMsg.Msg.GetSequence()); err != nil {
					return fmt.Errorf("unable to save state at seq=%d: %w", blockMsg.Msg.GetSequence(), err)
				}
				lastStateSaveTime = time.Now()
			}

			return nil
		},
		CorruptedBlockCb: func(ctx context.Context, msg messages.NatsMessage, decodingError error) (err error) {
			return fmt.Errorf("detected corrupted block at seq=%d (decoding err: %w)", msg.GetSequence(), decodingError)
		},
	})
	if err != nil {
		log.Fatalf("unable to start block-reader: %v", err)
	}
	defer func() {
		br.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("stream-validator is stopping"))
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ch:
		log.Printf("interrupted, stopping...")
	case <-br.Lifecycle().Ctx().Done():
		log.Fatalf("block-reader is stopping because of error: %v", br.Lifecycle().StoppingReason())
	}
}

func loadState() (uint64, error) {
	_, err := os.Stat(*stateFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("unable to stat state file at '%s': %w", *stateFile, err)
	}

	content, err := os.ReadFile(*stateFile)
	if err != nil {
		return 0, fmt.Errorf("unable to read state file at '%s': %w", *stateFile, err)
	}

	fields := strings.Fields(string(content))
	if len(fields) == 0 {
		log.Printf("WARN: state file is empty, will ignore")
		return 0, nil
	}

	seq, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		log.Printf("WARN: unable to parse content of state file at '%s' ('%s'): %v, will ignore", *stateFile, content, err)
		return 0, nil
	}
	return seq, nil
}

func saveState(seq uint64) error {
	return writeFileAtomically(*stateFile, []byte(strconv.FormatUint(seq, 10)))
}

func writeFileAtomically(path string, data []byte) error {
	tmpPath := filepath.Join(filepath.Dir(path), filepath.Base(path)+fmt.Sprintf(".tmp%d", time.Now().UnixNano()))
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func validate(prevBlock blocks.Block, nextBlock blocks.Block) error {
	if prevBlock == nil {
		return nil
	}

	if nextBlock.GetHeight() <= prevBlock.GetHeight() {
		return fmt.Errorf(
			"next block height (%d) is not higher than prev block height (%d)",
			nextBlock.GetHeight(), prevBlock.GetHeight(),
		)
	}
	if formats.Active().GetFormat() == formats.AuroraV2 && nextBlock.GetHeight()-prevBlock.GetHeight() != 1 {
		return fmt.Errorf(
			"next block height (%d) is not equal to prev block height (%d) + 1",
			nextBlock.GetHeight(), prevBlock.GetHeight(),
		)
	}

	h1 := prevBlock.GetHash()
	h2 := nextBlock.GetPrevHash()
	if formats.Active().GetFormat() == formats.AuroraV2 {
		h1 = strings.ToLower(h1)
		h2 = strings.ToLower(h2)
	}
	if h1 != h2 {
		return fmt.Errorf(
			"next block prev hash (%s) is not equal to prev block hash (%s)",
			h2, h1,
		)
	}

	return nil
}
