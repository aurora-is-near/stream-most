package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	lowerMsgIdHdr = strings.ToLower(jetstream.MsgIDHeader)
)

var (
	inputNats     = flag.String("input-nats", "", "input NATS endpoint(s), comma separated")
	inputCreds    = flag.String("input-creds", "", "input NATS creds file")
	inputStream   = flag.String("input-stream", "", "input NATS stream")
	inputStartSeq = flag.Uint64("input-start-seq", 1, "input start sequence")

	outputNats          = flag.String("output-nats", "", "output NATS endpoint(s), comma separated")
	outputCreds         = flag.String("output-creds", "", "output NATS creds file")
	outputStream        = flag.String("output-stream", "", "output NATS stream")
	outputSubject       = flag.String("output-subject", "", "output subject")
	outputStartSequence = flag.Uint64("output-start-seq", 1, "min output sequence to start looking state at")

	stateHeader = flag.String("state-header", "X-Scopier", "header in output messages for tracking state")
	dropMsgID   = flag.Bool("drop-msgid", false, "drop msgid headers")
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("finished with error: %v", err)
	}
}

func run() error {
	flag.Parse()
	if len(*outputSubject) == 0 {
		return fmt.Errorf("please provide -output-subject")
	}

	inSC, err := streamconnector.Connect(
		&streamconnector.Config{
			Nats: &transport.NATSConfig{
				ServerURL: *inputNats,
				Creds:     *inputCreds,
				LogTag:    "input",
			},
			Stream: &stream.Config{
				Name:        *inputStream,
				RequestWait: time.Second * 10,
				WriteWait:   time.Second * 10,
				LogTag:      "input-stream",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("unable to connect to input stream: %w", err)
	}
	defer inSC.Disconnect()

	outSC, err := streamconnector.Connect(
		&streamconnector.Config{
			Nats: &transport.NATSConfig{
				ServerURL: *outputNats,
				Creds:     *outputCreds,
				LogTag:    "output",
			},
			Stream: &stream.Config{
				Name:        *outputStream,
				RequestWait: time.Second * 10,
				WriteWait:   time.Second * 10,
				LogTag:      "output-stream",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("unable to connect to output stream: %w", err)
	}
	defer outSC.Disconnect()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)
	defer cancel()

	log.Printf("Looking for last copied message in output stream...")
	lastWrittenInputSeq, err := findLastWrittenInputSeq(ctx, outSC.Stream())
	if err != nil {
		return fmt.Errorf("unable to find last written input sequence")
	}
	log.Printf("Last written input seq=%d", lastWrittenInputSeq)

	nextInputSeq := max(*inputStartSeq, lastWrittenInputSeq+1)
	log.Printf("Will start copying from input seq=%d", nextInputSeq)

	var highestInputSeq atomic.Uint64

	lastLogTime := time.Now()
	msgsHandledSinceLastLog := uint64(0)

	rdr, err := reader.Start(
		inSC.Stream(),
		&reader.Config{
			Consumer: jetstream.OrderedConsumerConfig{
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:       nextInputSeq,
				ReplayPolicy:      jetstream.ReplayInstantPolicy,
				InactiveThreshold: time.Second * 10,
				HeadersOnly:       false,
			},
			StrictStart: true,
			MaxSilence:  time.Second * 10,
			LogTag:      "input-reader",
		},
		reader.NewCbReceiver().WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) error {
			if msg.GetSequence() != nextInputSeq {
				return fmt.Errorf("wrong msg seq, expected %d, got %d", nextInputSeq, msg.GetSequence())
			}
			nextInputSeq++

			headers := make(nats.Header, len(msg.GetHeader())+1)
			for k, v := range msg.GetHeader() {
				if lk := strings.ToLower(k); strings.HasPrefix(lk, "nats-") && (k != lowerMsgIdHdr || *dropMsgID) {
					continue
				}
				headers[k] = v
			}
			headers.Add(*stateHeader, strconv.FormatUint(msg.GetSequence(), 10))

			_, err := outSC.Stream().Write(ctx, &nats.Msg{
				Subject: *outputSubject,
				Header:  headers,
				Data:    msg.GetData(),
			})
			if err != nil {
				return fmt.Errorf("unable to write: %w", err)
			}

			msgsHandledSinceLastLog++
			if tDiff := time.Since(lastLogTime); tDiff.Seconds() > 3 {
				log.Printf(
					"At input seq=%d/%d, %.2fmsgs/sec",
					msg.GetSequence(),
					highestInputSeq.Load(),
					float64(msgsHandledSinceLastLog)/tDiff.Seconds(),
				)
				lastLogTime = time.Now()
				msgsHandledSinceLastLog = 0
			}

			return nil
		}).WithHandleFinishCb(func(err error) {
			cancel()
		}).WithHandleNewKnownSeqCb(func(ctx context.Context, seq uint64) error {
			highestInputSeq.Store(seq)
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("unable to start reader: %w", err)
	}
	defer rdr.Stop(nil, true)

	<-ctx.Done()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return rdr.Error()
}

func findLastWrittenInputSeq(ctx context.Context, outStream *stream.Stream) (uint64, error) {
	info, err := outStream.GetInfo(ctx)
	if err != nil {
		return 0, err
	}

	earliestChecked := info.State.LastSeq + 1

	for step := uint64(512); step <= 1048576; step *= 2 {
		log.Printf("Still looking... Will read up to %d msg-headers from end", step)
		info, err = outStream.GetInfo(ctx)
		if err != nil {
			return 0, err
		}

		checkStart := max(1, *outputStartSequence, info.State.FirstSeq)
		if earliestChecked <= checkStart {
			return 0, nil
		}
		if earliestChecked >= step {
			checkStart = max(checkStart, earliestChecked-step)
		}
		log.Printf("Will actually read range [%d;%d) now (%d message-headers)", checkStart, earliestChecked, earliestChecked-checkStart)

		lw, err := findLastWrittenInputSeqInRange(ctx, outStream, checkStart, earliestChecked)
		if err != nil {
			return 0, err
		}
		if lw != 0 {
			return lw, nil
		}

		earliestChecked = checkStart
	}

	return 0, fmt.Errorf("unable to find last written input sequence")
}

func findLastWrittenInputSeqInRange(ctx context.Context, outStream *stream.Stream, start uint64, end uint64) (uint64, error) {
	res := uint64(0)

	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rdr, err := reader.Start(
		outStream,
		&reader.Config{
			Consumer: jetstream.OrderedConsumerConfig{
				DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:       start,
				ReplayPolicy:      jetstream.ReplayInstantPolicy,
				InactiveThreshold: time.Second * 10,
				HeadersOnly:       true,
			},
			EndSeq:      end,
			StrictStart: false,
			MaxSilence:  time.Second * 10,
			LogTag:      "output-reader",
		},
		reader.NewCbReceiver().WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) error {
			val := msg.GetHeader().Get(*stateHeader)
			if len(val) == 0 {
				return nil
			}
			ival, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				log.Printf(
					"WARNING: got '%s' header on input seq=%d, but can't parse it, value='%s'",
					*stateHeader, msg.GetSequence(), val,
				)
			}
			res = max(res, ival)
			return nil
		}).WithHandleFinishCb(func(err error) {
			cancel()
		}),
	)
	if err != nil {
		return 0, err
	}
	defer rdr.Stop(nil, true)

	<-localCtx.Done()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if rdr.Error() != nil {
		return 0, rdr.Error()
	}
	return res, nil
}
