package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Connect to NATS & JetStream. Using `transport` package is optional,
	// any other preferred method will work as well.
	nc, err := transport.ConnectNATS(&transport.NATSConfig{
		OverrideURL: "nats://localhost:4222",
		Options:     transport.RecommendedNatsOptions(),
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
	rcv = rcv.WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) bool {

		// Handle message itself
		log.Printf("Got message with seq=%d on subject '%s': %s", msg.GetSequence(), msg.GetSubject(), msg.GetData())

		// We can tell reader to stop any time we want
		if string(msg.GetData()) == "halt" {
			log.Printf("Got special 'halt' message! Telling reader to stop consumption...")
			return false
		}

		// Or return true if we want to continue consumption
		return true
	})

	// Override receiver's tip sequence handler (optional)
	rcv = rcv.WithHandleNewKnownSeqCb(func(seq uint64) bool {

		log.Printf("Last known stream tip sequence is: %d", seq)

		// Return true to continue consumption
		return true
	})

	// Override receiver's finish handler (optional)
	readerFinished := make(chan struct{})
	rcv = rcv.WithHandleFinishCb(func(err error) {

		if err != nil {
			log.Printf("Reader finished because of error: %v", err)
		} else {
			log.Printf("Reader finished gracefully")
		}

		close(readerFinished)
	})

	// Start reader
	reader, err := reader.Start(
		inputStream,
		&reader.Config{

			// Optional. Provide specific subjects to read, otherwise it will read all subjects of given stream.
			FilterSubjects: []string{
				"a.b.c",
				"c.d.>",
				"e.f.*",
			},

			// Optional. Minimum sequence to start reading from. Zero-value defaults to 1.
			StartSeq: 5,

			// Optional. Sequence to stop at (exclusive). Zero-value means infinite reading.
			EndSeq: 100,

			// Optional. Means that reader should strictly start from StartSeq.
			// If first received sequence will be greater than StartSeq it will fail.
			StrictStart: true,

			// Optional. Max amount of time with no incoming messages after which health-check will be triggered.
			// Recommended to be greater than expected stream update rate.
			// Zero-value defaults to 5s.
			MaxSilence: time.Second * 5,

			// Optional. Use to elaborate logging if there are multiple active stream readers.
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
		reader.Stop(true)

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
