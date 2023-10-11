package streaminput

import (
	"errors"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

func makeCfg(natsUrl string, natslogTag string, streamName string, bufferSize uint, maxReconnects int, reconnectDelay time.Duration, stateFetchInterval time.Duration) *Config {
	return &Config{
		Conn: &streamconnector.Config{
			Nats: &transport.NATSConfig{
				OverrideURL: natsUrl,
				LogTag:      natslogTag,
				Options:     transport.RecommendedNatsOptions(),
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

	sIn := Start(makeCfg(s.ClientURL(), "streamin", "teststream", 1024, -1, time.Second/5, time.Second/5))
	defer sIn.Stop(true)

	// Wait until initial empty state is loaded
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 0) && u.RequireLastKnownMsg(t, lm, nil) && u.RequireSeq(t, lms, 0, 0)
	}, time.Second, time.Second/20)

	// Write first block
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(1, 555, "AAA", "_", nil),
	)

	// Wait until first block is loaded in state
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 1) && u.RequireLastKnownMsg(t, lm,
			nil,
			u.Announcement(1, 555, "AAA", "_", nil),
		) && u.RequireSeq(t, lms, 0, 1)
	}, time.Second, time.Second/20)

	// Write multiple new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Shard(2, 555, 0, "AAA", "_", nil),
		u.Shard(3, 555, 3, "AAA", "_", nil),
		u.Announcement(4, 557, "BBB", "AAA", nil),
		u.Announcement(5, 558, "CCC", "BBB", nil),
		u.Announcement(6, 600, "DDD", "CCC", nil),
	)

	// Make sure last block is loaded eventually
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 0, 1) && u.RequireSeq(t, ls, 1, 6) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(1, 555, "AAA", "_", nil),
			u.Shard(2, 555, 0, "AAA", "_", nil),
			u.Shard(3, 555, 3, "AAA", "_", nil),
			u.Announcement(4, 557, "BBB", "AAA", nil),
			u.Announcement(5, 558, "CCC", "BBB", nil),
			u.Announcement(6, 600, "DDD", "CCC", nil),
		) && u.RequireSeq(t, lms, 1, 6)
	}, time.Second, time.Second/20)

	// Shutdown server
	s.Shutdown()

	// Wait until streaminput has noticed that server has been shut down
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return true
		}
		u.RequireSeq(t, lds, 1, 1)
		u.RequireSeq(t, ls, 6, 6)
		u.RequireLastKnownMsg(t, lm, u.Announcement(6, 600, "DDD", "CCC", nil))
		u.RequireSeq(t, lms, 6, 6)
		return false
	}, time.Second*5, time.Second/20)

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
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 3, 3) && u.RequireSeq(t, ls, 8, 8) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(8, 608, "FFF", "EEE", nil),
		) && u.RequireSeq(t, lms, 8, 8)
	}, time.Second*5, time.Second/20)

	// Write a couple of new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(9, 609, "GGG", "FFF", nil),
		u.Announcement(10, 610, "HHH", "GGG", nil),
	)

	// Make sure that writes after restart are noticed as well
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 3, 5) && u.RequireSeq(t, ls, 8, 10) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(8, 608, "FFF", "EEE", nil),
			u.Announcement(9, 609, "GGG", "FFF", nil),
			u.Announcement(10, 610, "HHH", "GGG", nil),
		) && u.RequireSeq(t, lms, 8, 10)
	}, time.Second, time.Second/20)

	// Stop streaminput
	sIn.Stop(true)

	// Make sure that state eventually becomes completely unavailable
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrCompletelyUnavailable)
			return true
		}
		u.RequireSeq(t, lds, 5, 5)
		u.RequireSeq(t, ls, 10, 10)
		u.RequireLastKnownMsg(t, lm, u.Announcement(10, 610, "HHH", "GGG", nil))
		u.RequireSeq(t, lms, 10, 10)
		return false
	}, time.Second*5, time.Second/20)
}

func TestLimitedReconnects(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Start everything
	opts := u.GetTestNATSServerOpts(t)
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sIn := Start(makeCfg(s.ClientURL(), "streamin", "teststream", 1024, 2, time.Second/5, time.Second/5))
	defer sIn.Stop(true)

	// Wait until initial empty state is loaded
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 0) && u.RequireLastKnownMsg(t, lm, nil) && u.RequireSeq(t, lms, 0, 0)
	}, time.Second, time.Second/20)

	// Shutting down server
	s.Shutdown()

	// Wait until state becomes completely unavailable
	require.Eventually(t, func() bool {
		lds, ls, lm, lms, err := u.ExtractState(sIn)
		if err != nil {
			if errors.Is(err, blockio.ErrCompletelyUnavailable) {
				return true
			}
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		u.RequireSeq(t, lds, 0, 0)
		u.RequireSeq(t, ls, 0, 0)
		u.RequireLastKnownMsg(t, lm, nil)
		u.RequireSeq(t, lms, 0, 0)
		return false
	}, time.Second*5, time.Second/20)
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
