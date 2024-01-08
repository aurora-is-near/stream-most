package streaminput

import (
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats-server/v2/test"
)

func makeCfg(natsUrl string, natslogTag string, streamName string, bufferSize uint, maxReconnects int, reconnectDelay time.Duration, stateFetchInterval time.Duration) *Config {
	return &Config{
		Conn: &streamconnector.Config{
			Nats: &transport.NATSConfig{
				ServerURL: natsUrl,
				LogTag:    natslogTag,
			},
			Stream: &stream.Config{
				Name:        streamName,
				RequestWait: time.Second,
				WriteWait:   time.Second,
				LogTag:      natslogTag,
			},
		},
		MaxSilence:         time.Second,
		BufferSize:         bufferSize,
		MaxReconnects:      maxReconnects,
		ReconnectDelay:     reconnectDelay,
		StateFetchInterval: stateFetchInterval,
		LogInterval:        time.Second,
	}
}

func TestState(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Allocate two ports.
	// Second one will be needed to write a couple of new blocks while
	// stream is invisible for streaminput component.
	ports := u.GetFreePorts(2)
	opts := u.GetTestNATSServerOpts(t)
	opts.Port = ports[0]

	// Start everything
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sIn := Start(makeCfg(s.ClientURL(), "streamin", "teststream", 1024, -1, time.Second/5, time.Second/5), metrics.Dummy)
	defer sIn.Stop(true)

	// Wait until initial empty state is loaded
	u.WaitState(t, time.Second, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 0, Max: 0},
		LastSeq:        u.SeqRange{Min: 0, Max: 0},
		LastMsg:        []*messages.BlockMessage{nil},
		AllowedErrors:  []error{blockio.ErrTemporarilyUnavailable},
	})

	// Write first block
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(1, 555, "AAA", "_", nil),
	)

	// Wait until first block is loaded in state
	u.WaitState(t, time.Second, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 0, Max: 0},
		LastSeq:        u.SeqRange{Min: 0, Max: 1},
		LastMsg: []*messages.BlockMessage{
			nil,
			u.Announcement(1, 555, "AAA", "_", nil),
		},
	})

	// Write multiple new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Shard(2, 555, 0, "AAA", "_", nil),
		u.Shard(3, 555, 3, "AAA", "_", nil),
		u.Announcement(4, 557, "BBB", "AAA", nil),
		u.Announcement(5, 558, "CCC", "BBB", nil),
		u.Announcement(6, 600, "DDD", "CCC", nil),
	)

	// Make sure last block is loaded eventually
	u.WaitState(t, time.Second, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 0, Max: 1},
		LastSeq:        u.SeqRange{Min: 1, Max: 6},
		LastMsg: []*messages.BlockMessage{
			u.Announcement(1, 555, "AAA", "_", nil),
			u.Shard(2, 555, 0, "AAA", "_", nil),
			u.Shard(3, 555, 3, "AAA", "_", nil),
			u.Announcement(4, 557, "BBB", "AAA", nil),
			u.Announcement(5, 558, "CCC", "BBB", nil),
			u.Announcement(6, 600, "DDD", "CCC", nil),
		},
	})

	// Shutdown server
	s.Shutdown()

	// Wait until streaminput has noticed that server has been shut down
	u.WaitState(t, time.Second*5, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 1, Max: 1},
		LastSeq:        u.SeqRange{Min: 6, Max: 6},
		LastMsg: []*messages.BlockMessage{
			u.Announcement(6, 600, "DDD", "CCC", nil),
		},
		RequiredErrors: []error{blockio.ErrTemporarilyUnavailable},
	})

	// Start server on another port to make a couple of writes while
	// stream is invisible for streaminput
	opts.Port = ports[1]
	s = test.RunServer(&opts)

	// Write a couple of new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(7, 607, "EEE", "DDD", nil),
		u.Announcement(8, 608, "FFF", "EEE", nil),
	)

	// Restart it on initial port again
	s.Shutdown()
	opts.Port = ports[0]
	s = test.RunServer(&opts)

	// Make sure that new blocks are eventually noticed by streaminput
	u.WaitState(t, time.Second*5, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 3, Max: 3},
		LastSeq:        u.SeqRange{Min: 8, Max: 8},
		LastMsg: []*messages.BlockMessage{
			u.Announcement(8, 608, "FFF", "EEE", nil),
		},
		AllowedErrors: []error{blockio.ErrTemporarilyUnavailable},
	})

	// Write a couple of new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(9, 609, "GGG", "FFF", nil),
		u.Announcement(10, 610, "HHH", "GGG", nil),
	)

	// Make sure that writes after restart are noticed as well
	u.WaitState(t, time.Second, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 3, Max: 5},
		LastSeq:        u.SeqRange{Min: 8, Max: 10},
		LastMsg: []*messages.BlockMessage{
			u.Announcement(8, 608, "FFF", "EEE", nil),
			u.Announcement(9, 609, "GGG", "FFF", nil),
			u.Announcement(10, 610, "HHH", "GGG", nil),
		},
	})

	// Stop streaminput
	sIn.Stop(true)

	// Make sure that state eventually becomes completely unavailable
	u.WaitState(t, time.Second*5, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 5, Max: 5},
		LastSeq:        u.SeqRange{Min: 10, Max: 10},
		LastMsg:        []*messages.BlockMessage{u.Announcement(10, 610, "HHH", "GGG", nil)},
		AllowedErrors:  []error{blockio.ErrTemporarilyUnavailable},
		RequiredErrors: []error{blockio.ErrCompletelyUnavailable},
	})
}

func TestLimitedReconnects(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Start everything
	opts := u.GetTestNATSServerOpts(t)
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sIn := Start(makeCfg(s.ClientURL(), "streamin", "teststream", 1024, 2, time.Second/5, time.Second/5), metrics.Dummy)
	defer sIn.Stop(true)

	// Wait until initial empty state is loaded
	u.WaitState(t, time.Second, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 0, Max: 0},
		LastSeq:        u.SeqRange{Min: 0, Max: 0},
		LastMsg:        []*messages.BlockMessage{nil},
		AllowedErrors:  []error{blockio.ErrTemporarilyUnavailable},
	})

	// Shutting down server
	s.Shutdown()

	// Wait until state becomes completely unavailable
	u.WaitState(t, time.Second*5, sIn, &u.ExpectedState{
		LastDeletedSeq: u.SeqRange{Min: 0, Max: 0},
		LastSeq:        u.SeqRange{Min: 0, Max: 0},
		LastMsg:        []*messages.BlockMessage{nil},
		AllowedErrors:  []error{blockio.ErrTemporarilyUnavailable},
		RequiredErrors: []error{blockio.ErrCompletelyUnavailable},
	})
}

/*
	TODO:
		1. Session survives reconnect (+ invisible writes)
		2. Session doesn't survive reconnect
		3. Session ends gracefully
		4. Session ends gracefully on seek
		5. Reseek before session end
		6. Reseek after session end
		7. Reseek during reconnect (+ invisible writes)
		8. Reseek after stop
		9. Reseek after connection death
		10. Reading affects state
*/

/*
	TODO: fill remaining misc test-coverage gaps

	go test -v -coverprofile cover.out .
	go tool cover -html cover.out -o cover.html
*/
