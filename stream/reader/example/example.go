package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Connect to NATS & JetStream. Using `transport` package is optional,
	// any other preferred method will work as well.
	nc, err := transport.ConnectNATS(&transport.NATSConfig{
		ServerURL: "nats://localhost:4222",
	})
	if err != nil {
		panic(err)
	}
	defer nc.Drain()
	js, err := jetstream.New(nc.Conn())
	if err != nil {
		panic(err)
	}

	// Get stream handler
	inputStream, err := stream.Connect(&stream.Config{

		// Name of the stream
		Name: "mystream",

		// Optional. Max timeout for read-requests like get-info. Zero-value defaults to 10s.
		RequestWait: time.Second * 10,

		// Optional. Max timeout for write-requests (not applicable for this example). Zero-value defaults to 10s.
		WriteWait: time.Second * 10,

		// Optional. Use to elaborate logging if there are multiple active stream handlers.
		LogTag: "mystream",
	}, js)
	if err != nil {
		panic(err)
	}

	// Create receiver for getting data from reader
	rcv := reader.NewCbReceiver()

	// Override receiver's message handler (optional)
	rcv = rcv.WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) error {

		// Handle message itself
		log.Printf("Got message with seq=%d on subject '%s': %s", msg.GetSequence(), msg.GetSubject(), msg.GetData())

		// We can tell reader to stop any time we want
		if string(msg.GetData()) == "halt" {
			log.Printf("Got special 'halt' message! Telling reader to stop consumption...")
			return fmt.Errorf("special halt message detected")
		}

		// Or return nil if we want to continue consumption
		return nil
	})

	// Override receiver's tip sequence handler (optional)
	rcv = rcv.WithHandleNewKnownSeqCb(func(ctx context.Context, seq uint64) error {

		log.Printf("Last known stream tip sequence is: %d", seq)

		// Return nil to continue consumption
		return nil
	})

	// Override receiver's finish handler (optional)
	readerFinished := make(chan struct{})
	rcv = rcv.WithHandleFinishCb(func(err error) {

		switch {
		case err == nil:
			log.Printf("Reader finished gracefully")
		case errors.Is(err, reader.ErrInterrupted):
			log.Printf("Reader finished because it was interrupted: %v", err)
		default:
			log.Printf("Reader finished because of error: %v", err)
		}

		close(readerFinished)
	})

	// Start reader
	reader, err := reader.Start(
		inputStream,
		&reader.Config{

			// Inner ordered consumer configuration
			Consumer: jetstream.OrderedConsumerConfig{
				// Minimum sequence to start reading from. Zero-value defaults to 1
				OptStartSeq: 5,

				// See OrderedConsumerConfig in github.com/nats-io/nats.go/jetstream
				// for other useful fields such as FilterSubjects, OptStartTime, DeliverPolicy, HeadersOnly etc
			},

			// Optional. Consumer fine-tuning. Default (empty array or nil) works perfect
			// See github.com/nats-io/nats.go/jetstream for details
			PullOpts: []jetstream.PullConsumeOpt{},

			// Optional. Sequence to stop at (exclusive). Zero-value means no end sequence
			EndSeq: 100,

			// Optional. Msg-time to stop at (exclusive). Nil-value means no end time
			EndTime: util.Ptr(time.Now()),

			// Optional. Means that reader should strictly start from StartSeq
			// If first received sequence will be greater than StartSeq it will fail
			StrictStart: true,

			// Optional. Max amount of time with no incoming messages after which health-check will be triggered
			// Recommended to be greater than expected stream update rate
			// Zero-value defaults to 5s.
			MaxSilence: time.Second * 5,

			// Optional. Use to elaborate logging if there are multiple active stream readers
			LogTag: "mystream-reader",
		},
		rcv,
	)
	if err != nil {
		panic(err)
	}

	// Defer reader stopping
	defer func() {
		// Wait for reader to stop
		reader.Stop(nil, true)

		// Check reader error
		if reader.Error() != nil {
			log.Printf("Reader finished because of error: %v", reader.Error())
		} else {
			log.Printf("Reader finished gracefully")
		}
	}()

	// Wait until either we get interruption signal or reader itself finishes
	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)
	select {
	case <-interrupt:
		log.Printf("Interrupted")
	case <-readerFinished:
	}
}
