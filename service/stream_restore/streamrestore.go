package stream_restore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"

	"github.com/aurora-is-near/stream-backup/chunks"
)

const stdoutInterval = time.Second * 5

var errConnectionProblem = errors.New("connection problem")
var errInterrupted = errors.New("interrupted")
var errStartNotFound = errors.New("start not found")

type StreamRestore struct {
	Mode            string
	Chunks          chunks.ChunksInterface
	Stream          *stream.Options
	Writer          *block_writer.Options
	StartSeq        uint64
	EndSeq          uint64
	ToleranceWindow uint
	ReconnectWaitMs uint

	interrupt chan os.Signal
}

func (sr *StreamRestore) Run() error {
	if sr.StartSeq >= sr.EndSeq {
		return fmt.Errorf("StartSeq must be less than EndSeq")
	}

	if err := sr.Chunks.Open(); err != nil {
		return err
	}
	defer sr.Chunks.CloseReader()

	sr.interrupt = make(chan os.Signal, 10)
	signal.Notify(sr.interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	for {
		switch err := sr.push(); err {
		case nil:
			log.Printf("Finished")
			return nil
		case errInterrupted:
			log.Printf("Interrupted")
			return nil
		case errConnectionProblem:
			timer := time.NewTimer(time.Duration(sr.ReconnectWaitMs) * time.Millisecond)
			log.Printf("Sleeping for %vms before next reconnection...", sr.ReconnectWaitMs)
			select {
			case <-sr.interrupt:
				timer.Stop()
				log.Printf("Interrupted")
				return nil
			case <-timer.C:
				continue
			}
		default:
			return err
		}
	}
}

func (sr *StreamRestore) push() error {
	if sr.isInterrupted() {
		return errInterrupted
	}

	str, err := stream.Connect(sr.Stream)
	if err != nil {
		log.Printf("Got connection problem, will reconnect: %v", err)
		return errConnectionProblem
	}

	if sr.isInterrupted() {
		return errInterrupted
	}

	streamPeek := stream_peek.NewStreamPeek(str)

	writer := block_writer.NewWriter(sr.Writer, str, streamPeek)
	if err != nil {
		log.Printf("Got connection problem, will reconnect: %v", err)
		return errConnectionProblem
	}

	tip, err := streamPeek.GetTip()
	if err != nil {
		log.Printf("Got connection problem, will reconnect: %v", err)
		return errConnectionProblem
	}

	var tipBlock blocks.Block
	if tip != nil {
		tipBlock = tip.Block
	}

	seq, err := sr.findStartChunkSeq(tipBlock)
	if err == errStartNotFound {
		log.Printf("Nothing to do")
		return nil
	}
	if err == errInterrupted {
		return errInterrupted
	}
	if err != nil {
		return fmt.Errorf("can't figure start chunk: %w", err)
	}

	output, stopReading := sr.startReading(seq)
	defer stopReading()

	lastReadSeq, lastWrittenSeq := uint64(0), uint64(0)
	lastStdout := time.Now()

	consecutiveWrongBlocks := 0
	lastBlockWasWrong := false
	for {
		if sr.isInterrupted() {
			return errInterrupted
		}

		if lastBlockWasWrong {
			consecutiveWrongBlocks++
			if consecutiveWrongBlocks > int(sr.ToleranceWindow) {
				return fmt.Errorf("tolerance window exceeded")
			}
			lastBlockWasWrong = false
		} else {
			consecutiveWrongBlocks = 0
		}

		if time.Since(lastStdout) > stdoutInterval {
			fmt.Printf("[STATE]: lastReadSeq=%v, lastWrittenSeq=%v\n", lastReadSeq, lastWrittenSeq)
			lastStdout = time.Now()
		}

		select {
		case <-sr.interrupt:
			return errInterrupted
		case out, ok := <-output:
			if !ok {
				return nil
			}
			if out.err != nil {
				return fmt.Errorf("can't read from backup: %w", out.err)
			}
			bb := out.blockBackup
			lastReadSeq = bb.Sequence
			switch ack, err := writer.WriteWithAck(context.Background(), &messages.BlockMessage{
				Block: bb.Block,
				Msg: &messages.RawStreamMessage{
					RawStreamMsg: &jetstream.RawStreamMsg{
						Data: bb.MessageBackup.Data,
						// TODO: headers
					},
				},
			}); err {
			case nil:
				if ack != nil {
					lastWrittenSeq = ack.Sequence
				}
			case block_writer.ErrDuplicate:
				logrus.Warn("Duplicate block")
			case block_writer.ErrHashMismatch:
				fmt.Printf("[WRONGHASH]: seq=%v\n", bb.Sequence)
				lastBlockWasWrong = true
			case block_writer.ErrLowHeight:
			default:
				log.Printf("Got writer problem, will restart connection: %v", err)
				return errConnectionProblem
			}
		}
	}
}

func (sr *StreamRestore) findStartChunkSeq(tip blocks.Block) (uint64, error) {
	tipHeight := uint64(0)
	if tip != nil {
		tipHeight = tip.GetHeight()
	}

	log.Printf("Getting chunks info...")
	chunkRanges := sr.Chunks.GetChunkRanges()
	chunkStarts := []uint64{}
	for _, cr := range chunkRanges {
		if cr.R < sr.StartSeq || cr.L >= sr.EndSeq {
			continue
		}
		chunkStarts = append(chunkStarts, cr.L)
	}
	if len(chunkStarts) == 0 {
		return 0, errStartNotFound
	}
	sort.Slice(chunkStarts, func(i, j int) bool {
		return chunkStarts[i] < chunkStarts[j]
	})

	log.Printf("Binsearching for the chunk to start with...")
	l, r := 0, len(chunkStarts)
	for l+1 < r {
		if sr.isInterrupted() {
			return 0, errInterrupted
		}

		log.Printf("Binsearch state: [%d;%d)", l, r)
		m := (l + r) / 2

		bb, err := sr.readSingle(chunkStarts[m])
		if err != nil {
			return 0, fmt.Errorf("binsearch: can't read first message of chunk which starts on %v: %w", chunkStarts[m], err)
		}
		if bb.Sequence != chunkStarts[m] {
			return 0, fmt.Errorf("binsearch: bb.Sequence != chunkStarts[m]")
		}
		if bb.Block.GetHeight() <= tipHeight {
			l = m
		} else {
			r = m
		}
	}

	log.Printf("Will start with chunk which starts on %v seq", chunkStarts[l])
	return chunkStarts[l], nil
}

func (sr *StreamRestore) isInterrupted() bool {
	select {
	case <-sr.interrupt:
		return true
	default:
		return false
	}
}
