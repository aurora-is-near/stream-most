package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
)

var errRetryable = errors.New("retryable")

var (
	natsUrl      = flag.String("nats", "", "NATS endpoint(s), comma separated")
	natsCreds    = flag.String("creds", "", "NATS creds file")
	streamName   = flag.String("stream", "", "NATS stream name")
	sizeLimitKB  = flag.Int64("size-limit-kb", 0, "target size limit in kilobytes")
	sizeLimitMB  = flag.Int64("size-limit-mb", 0, "target size limit in megabytes")
	sizeLimitGB  = flag.Int64("size-limit-gb", 0, "target size limit in gigabytes")
	maxSpeedMBps = flag.Int64("max-speed-mbps", 50, "max shrinking speed in megabytes per second")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("finished with error: %v", err)
	}
}

func run() error {
	var targetSizeLimitBytes int64
	if *sizeLimitKB > 0 {
		targetSizeLimitBytes = *sizeLimitKB * 1024
	}
	if *sizeLimitMB > 0 {
		if targetSizeLimitBytes > 0 {
			return fmt.Errorf("only one size limit must be provided")
		}
		targetSizeLimitBytes = *sizeLimitMB * 1024 * 1024
	}
	if *sizeLimitGB > 0 {
		if targetSizeLimitBytes > 0 {
			return fmt.Errorf("only one size limit must be provided")
		}
		targetSizeLimitBytes = *sizeLimitGB * 1024 * 1024 * 1024
	}
	if targetSizeLimitBytes == 0 {
		return fmt.Errorf("non-zero size limit must be provided")
	}
	if *maxSpeedMBps <= 0 {
		return fmt.Errorf("non-zero max-speed-mbps must be provided")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	defer cancel()

	iterationTicker := time.NewTicker(time.Second)
	defer iterationTicker.Stop()

	i := 0
	for {
		i++
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		case <-iterationTicker.C:
			err := runConnIteration(ctx, targetSizeLimitBytes)
			if !errors.Is(err, errRetryable) {
				return err
			}
			log.Printf("error on reconnection iteration #%d: %v", i, err)
		}
	}
}

func runConnIteration(ctx context.Context, targetSizeLimitBytes int64) error {
	sc, err := streamconnector.Connect(
		&streamconnector.Config{
			Nats: &transport.NATSConfig{
				ServerURL: *natsUrl,
				Creds:     *natsCreds,
				LogTag:    "input",
			},
			Stream: &stream.Config{
				Name:        *streamName,
				RequestWait: time.Second * 10,
				WriteWait:   time.Second * 10,
				LogTag:      "input-stream",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("unable to connect to input stream: %w (%w)", err, errRetryable)
	}
	defer sc.Disconnect()

	maxSpeedBytes := *maxSpeedMBps * 1024 * 1024

	stepTicker := time.NewTicker(time.Second)
	defer stepTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}

		info, err := sc.Stream().GetInfo(ctx)
		if err != nil {
			return fmt.Errorf("unable to get stream info: %w (%w)", err, errRetryable)
		}

		cfg := info.Config
		var nextStep int64
		if cfg.MaxBytes <= 0 {
			log.Printf("Current size limit: unlimited")
			log.Printf(
				"Current size: %d bytes = %0.2f kilobytes = %0.2f megabytes = %0.2f gigabytes",
				info.State.Bytes,
				float64(info.State.Bytes)/1024.0,
				float64(info.State.Bytes)/1024.0/1024.0,
				float64(info.State.Bytes)/1024.0/1024.0/1024.0,
			)
			if info.State.Bytes > uint64(targetSizeLimitBytes) {
				nextStep = int64(info.State.Bytes)
			} else {
				nextStep = targetSizeLimitBytes
			}
		} else {
			log.Printf(
				"Current size limit: %d bytes = %0.2f kilobytes = %0.2f megabytes = %0.2f gigabytes",
				cfg.MaxBytes,
				float64(cfg.MaxBytes)/1024.0,
				float64(cfg.MaxBytes)/1024.0/1024.0,
				float64(cfg.MaxBytes)/1024.0/1024.0/1024.0,
			)
			if cfg.MaxBytes <= targetSizeLimitBytes {
				log.Printf("Done!")
				return nil
			}
			nextStep = max(targetSizeLimitBytes, cfg.MaxBytes-maxSpeedBytes)
			eta := float64(cfg.MaxBytes-targetSizeLimitBytes) / float64(maxSpeedBytes)
			etaDur := time.Duration(eta * float64(time.Second))
			log.Printf("ETA: %s", etaDur.String())
		}

		log.Printf(
			"Next step: %d bytes = %0.2f kilobytes = %0.2f megabytes = %0.2f gigabytes",
			nextStep,
			float64(nextStep)/1024.0,
			float64(nextStep)/1024.0/1024.0,
			float64(nextStep)/1024.0/1024.0/1024.0,
		)

		cfg.MaxBytes = nextStep
		_, err = sc.Stream().Js().UpdateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("unable to update stream: %w (%w)", err, errRetryable)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		case <-stepTicker.C:
		}
	}
}
