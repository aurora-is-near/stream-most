package stream_backup

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/autoreader"

	"github.com/aurora-is-near/stream-backup/chunks"
	"github.com/aurora-is-near/stream-backup/messagebackup"
)

const stdoutInterval = time.Second * 5

var errInterrupted = errors.New("interrupted")

type StreamBackup struct {
	Chunks   chunks.ChunksInterface
	Reader   *autoreader.AutoReader[messages.BlockMessage]
	StartSeq uint64
	EndSeq   uint64

	interrupt chan os.Signal
}

func (sb *StreamBackup) Run() error {
	log.Printf("StreamBackup: opening chunks dir...")
	if err := sb.Chunks.Open(); err != nil {
		return err
	}
	defer sb.Chunks.Flush()
	defer sb.Chunks.CloseReader()
	defer log.Printf("StreamBackup: closing chunks...")

	sb.interrupt = make(chan os.Signal, 10)
	signal.Notify(sb.interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)

	for {
		select {
		case <-sb.interrupt:
			return nil
		default:
		}

		l, r, err := sb.Chunks.GetLeftmostAbsentRange(sb.StartSeq, sb.EndSeq-1)
		if err == chunks.ErrNotFound {
			log.Printf("Finished")
			return nil
		}
		if err != nil {
			return fmt.Errorf("can't figure out next range: %w", err)
		}

		log.Printf("Pulling segment [%d, %d]", l, r)
		err = sb.pullSegment(l, r)
		if err == errInterrupted {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (sb *StreamBackup) pullSegment(l, r uint64) error {
	var prevBlock blocks.Block
	if l > sb.StartSeq {
		if err := sb.Chunks.SeekReader(l - 1); err != nil {
			return fmt.Errorf("can't seek to prev block: %w", err)
		}
		prev, prevData, err := sb.Chunks.ReadNext()
		sb.Chunks.CloseReader()
		if err != nil {
			return fmt.Errorf("can't read prev block: %w", err)
		}
		if prev != l-1 {
			return fmt.Errorf("prev != l - 1")
		}
		mb := &messagebackup.MessageBackup{}
		if err := mb.UnmarshalVT(prevData); err != nil {
			return fmt.Errorf("can't unmarshal prev block: %w", err)
		}
		prevBlock, err = formats.Active().ParseBlock(mb.Data)
		if err != nil {
			return fmt.Errorf("can't decode prev block: %v", err)
		}
	}

	sb.Reader.Start(l, r+1)
	defer sb.Reader.Stop()

	stdoutTicker := time.NewTicker(stdoutInterval)
	defer stdoutTicker.Stop()

	for {
		if l > r {
			return nil
		}

		select {
		case <-sb.interrupt:
			return errInterrupted
		default:
		}

		select {
		case <-sb.interrupt:
			return errInterrupted
		case <-stdoutTicker.C:
			fmt.Printf("%v [STATE] curSeq=%v\n", time.Now().Format(time.RFC3339), l)
		case curMsg, ok := <-sb.Reader.Output():
			if !ok {
				return nil
			}
			curBlock, err := curMsg.Value()
			if err != nil {
				return fmt.Errorf("can't decode new block on seq %v: %w", curMsg.Msg().GetSequence(), err)
			}
			if prevBlock != nil {
				switch formats.Active().GetFormat() {
				// TODO: add "simple" format
				case formats.AuroraV2, formats.NearV2:
					if prevBlock.GetHash() != curBlock.GetPrevHash() {
						return fmt.Errorf("hash mismatch on seq %v", curBlock.GetSequence())
					}
					if prevBlock.GetHeight() >= curBlock.GetHeight() {
						return fmt.Errorf("height mismatch on seq %v", curBlock.GetSequence())
					}
				case formats.NearV3:
					if prevBlock.GetHash() == curBlock.GetHash() {
						if _, ok := prevBlock.(*v3.NearBlockAnnouncement); !ok {
							return fmt.Errorf("unexpected near v3 block on seq %d after shard on prev block", curBlock.GetSequence())
						}
						if curBlock.GetType() != messages.Shard {
							return fmt.Errorf("near v3 block on seq %d expected to be shard but it's not", curBlock.GetSequence())
						}
						if prevBlock.GetHeight() != curBlock.GetHeight() {
							return fmt.Errorf("height mismatch on seq %v", curBlock.GetSequence())
						}
					} else {
						if prevBlock.GetHash() != curBlock.GetPrevHash() {
							return fmt.Errorf("hash mismatch on seq %v", curBlock.GetSequence())
						}
						if prevBlock.GetHeight() >= curBlock.GetHeight() {
							return fmt.Errorf("height mismatch on seq %v", curBlock.GetSequence())
						}
						if curBlock.GetType() != messages.Announcement {
							return fmt.Errorf("near v3 block on seq %d expected to be announcement but it's not", curBlock.GetSequence())
						}
					}
				}
			}
			mb := &messagebackup.MessageBackup{
				Headers:  make(map[string]*messagebackup.HeaderValues),
				UnixNano: uint64(curBlock.GetTimestamp().UnixNano()),
				Data:     curBlock.GetData(),
				Sequence: curBlock.GetSequence(),
			}
			for header, values := range curBlock.GetHeader() {
				mb.Headers[header] = &messagebackup.HeaderValues{Values: values}
			}
			data, err := mb.MarshalVT()
			if err != nil {
				return fmt.Errorf("can't marshal new block on seq %v: %w", curBlock.GetSequence(), err)
			}
			if err := sb.Chunks.Write(l, data); err != nil {
				return fmt.Errorf("can't write new block on seq %v: %w", curBlock.GetSequence(), err)
			}
			prevBlock = curBlock.GetBlock()
			l++
		}
	}
}
