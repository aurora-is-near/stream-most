package streamoutput

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func makeCfg(natsUrl string, natsLogTag string, streamName string, maxReconnects int, reconnectDelay time.Duration, stateFetchInterval time.Duration) *Config {
	return &Config{
		Conn: &streamconnector.Config{
			Nats: &transport.NATSConfig{
				OverrideURL: natsUrl,
				LogTag:      natsLogTag,
				Options:     transport.RecommendedNatsOptions(),
			},
			Stream: &stream.Config{
				Name:        streamName,
				RequestWait: time.Second,
				WriteWait:   time.Second,
				LogTag:      natsLogTag,
			},
		},
		WriteRetryWait:        jetstream.DefaultPubRetryWait,
		WriteRetryAttempts:    jetstream.DefaultPubRetryAttempts,
		PreserveCustomHeaders: true,
		MaxReconnects:         maxReconnects,
		ReconnectDelay:        reconnectDelay,
		StateFetchInterval:    stateFetchInterval,
	}
}

func TestState(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Allocate two ports.
	// Second one will be needed to write a couple of new blocks while
	// stream is invisible for streamoutput component.
	ports := u.GetFreePorts(2)
	opts := u.GetTestNATSServerOpts(t)
	opts.Port = ports[0]

	// Start everything
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sOut := Start(makeCfg(s.ClientURL(), "streamout", "teststream", -1, time.Second/5, time.Second/5))
	defer sOut.Stop(true)

	// Wait until initial empty state is loaded
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 0) && u.RequireLastKnownMsg(t, lm, nil)
	}, time.Second, time.Second/20)

	// Write first block
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(1, 555, "AAA", "_", nil),
	)

	// Wait until first block is loaded in state
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 1) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(1, 555, "AAA", "_", nil),
		)
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
		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 0, 1) && u.RequireSeq(t, ls, 1, 6) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(1, 555, "AAA", "_", nil),
			u.Shard(2, 555, 0, "AAA", "_", nil),
			u.Shard(3, 555, 3, "AAA", "_", nil),
			u.Announcement(4, 557, "BBB", "AAA", nil),
			u.Announcement(5, 558, "CCC", "BBB", nil),
			u.Announcement(6, 600, "DDD", "CCC", nil),
		)
	}, time.Second, time.Second/20)

	// Shutdown server
	s.Shutdown()

	// Wait until streamoutput has noticed that server has been shut down
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return true
		}
		u.RequireSeq(t, lds, 1, 1)
		u.RequireSeq(t, ls, 6, 6)
		u.RequireLastKnownMsg(t, lm, u.Announcement(6, 600, "DDD", "CCC", nil))
		return false
	}, time.Second*5, time.Second/20)

	// Start server on another port to make a couple of writes while
	// stream is invisible for streamoutput
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

	// Make sure that new blocks are eventually noticed by streamoutput
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 3, 3) && u.RequireSeq(t, ls, 8, 8) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(8, 608, "FFF", "EEE", nil),
		)
	}, time.Second*5, time.Second/20)

	// Write a couple of new blocks
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(9, 609, "GGG", "FFF", nil),
		u.Announcement(10, 610, "HHH", "GGG", nil),
	)

	// Make sure that writes after restart are noticed as well
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 3, 5) && u.RequireSeq(t, ls, 8, 10) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(8, 608, "FFF", "EEE", nil),
			u.Announcement(9, 609, "GGG", "FFF", nil),
			u.Announcement(10, 610, "HHH", "GGG", nil),
		)
	}, time.Second, time.Second/20)

	// Stop streamoutput
	sOut.Stop(true)

	// Make sure that state eventually becomes completely unavailable
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrCompletelyUnavailable)
			return true
		}
		u.RequireSeq(t, lds, 5, 5)
		u.RequireSeq(t, ls, 10, 10)
		u.RequireLastKnownMsg(t, lm, u.Announcement(10, 610, "HHH", "GGG", nil))
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

	sOut := Start(makeCfg(s.ClientURL(), "streamout", "teststream", 2, time.Second/5, time.Second/5))
	defer sOut.Stop(true)

	// Wait until initial empty state is loaded
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 0) && u.RequireLastKnownMsg(t, lm, nil)
	}, time.Second, time.Second/20)

	// Shutting down server
	s.Shutdown()

	// Wait until state becomes completely unavailable
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
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
		return false
	}, time.Second*5, time.Second/20)
}

func TestProtectedWriteReconnect(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Start everything
	opts := u.GetTestNATSServerOpts(t)
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sOut := Start(makeCfg(s.ClientURL(), "streamout", "teststream", -1, time.Second/5, time.Second/5))
	defer sOut.Stop(true)

	// Wait until first write succeeds
	require.Eventually(t, func() bool {
		err := sOut.ProtectedWrite(context.Background(), 0, "", u.Announcement(1, 101, "AAA", "_", nil))
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return true
	}, time.Second*2, time.Second/20)

	// Check that next write succeeds
	err := sOut.ProtectedWrite(context.Background(), 1, "101", u.Shard(2, 101, 3, "AAA", "_", nil))
	require.NoError(t, err)

	// Stop server
	s.Shutdown()

	// Make sure writes don't work now
	err = sOut.ProtectedWrite(context.Background(), 2, "101.3", u.Shard(3, 101, 5, "AAA", "_", nil))
	require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)

	// Run server again
	s = test.RunServer(&opts)
	_ = s

	// Wait until writes work again
	require.Eventually(t, func() bool {
		err := sOut.ProtectedWrite(context.Background(), 2, "101.3", u.Shard(3, 101, 5, "AAA", "_", nil))
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return true
	}, time.Second*2, time.Second/20)

	// Check that next write succeeds as well
	err = sOut.ProtectedWrite(context.Background(), 3, "101.5", u.Announcement(4, 105, "BBB", "AAA", nil))
	require.NoError(t, err)
}

func TestProtectedWriteAffectsState(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Start everything
	opts := u.GetTestNATSServerOpts(t)
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 5, time.Hour)

	sOut := Start(makeCfg(s.ClientURL(), "streamout", "teststream", -1, time.Second/5, time.Second))
	defer sOut.Stop(true)

	// Wait until initial empty state is loaded
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 0, 0) && u.RequireSeq(t, ls, 0, 0) && u.RequireLastKnownMsg(t, lm, nil)
	}, time.Second*5, time.Second/20)

	// Test that every write immediately affects the state
	lastMsgID := ""
	for i := uint64(1); i <= 100; i++ {
		newBlock := u.Announcement(i, i+100, toHex(i+100), toHex(i+99), nil)
		err := sOut.ProtectedWrite(context.Background(), i-1, lastMsgID, newBlock)
		require.NoError(t, err)
		lastMsgID = blocks.ConstructMsgID(newBlock.Block)

		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)

		maxLds := uint64(0)
		if i > 5 {
			maxLds = i - 5
		}
		u.RequireSeq(t, lds, 0, maxLds)
		u.RequireSeq(t, ls, i, i)
		u.RequireLastKnownMsg(t, lm, newBlock)
	}

	// Do external write
	u.WriteBlocks(s.ClientURL(), "teststream", "teststream.*", true,
		u.Announcement(101, 201, toHex(201), toHex(200), nil),
	)

	// Makes sure external write affects state as well
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)
		return u.RequireSeq(t, lds, 0, 96) && u.RequireSeq(t, ls, 100, 101) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(100, 200, toHex(200), toHex(199), nil),
			u.Announcement(101, 201, toHex(201), toHex(200), nil),
		)
	}, time.Second*5, time.Second/20)

	// Stop server
	s.Shutdown()

	// Wait until streamoutput has noticed that server has been shut down
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return true
		}
		u.RequireSeq(t, lds, 96, 96)
		u.RequireSeq(t, ls, 101, 101)
		u.RequireLastKnownMsg(t, lm, u.Announcement(101, 201, toHex(201), toHex(200), nil))
		return false
	}, time.Second*5, time.Second/20)

	// Run server again
	s = test.RunServer(&opts)
	_ = s

	// Wait until streamoutput state is loaded again
	require.Eventually(t, func() bool {
		lds, ls, lm, err := u.ExtractState(sOut)
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		return u.RequireSeq(t, lds, 96, 96) && u.RequireSeq(t, ls, 101, 101) && u.RequireLastKnownMsg(t, lm,
			u.Announcement(101, 201, toHex(201), toHex(200), nil),
		)
	}, time.Second*5, time.Second/20)

	// Test that every write immediately affects the state again
	lastMsgID = "201"
	for i := uint64(102); i <= 200; i++ {
		newBlock := u.Shard(i, 201, i, toHex(i+100), toHex(i+99), nil)
		err := sOut.ProtectedWrite(context.Background(), i-1, lastMsgID, newBlock)
		require.NoError(t, err)
		lastMsgID = blocks.ConstructMsgID(newBlock.Block)

		lds, ls, lm, err := u.ExtractState(sOut)
		require.NoError(t, err)
		u.RequireSeq(t, lds, 96, i-5)
		u.RequireSeq(t, ls, i, i)
		u.RequireLastKnownMsg(t, lm, newBlock)
	}

	// Write duplicate
	duplicateBlock := u.Shard(198, 201, 198, toHex(298), toHex(297), nil)
	err := sOut.ProtectedWrite(context.Background(), 197, "201.197", duplicateBlock)
	require.NoError(t, err)

	// Make sure that duplicate write doesn't affect the state
	lds, ls, lm, err := u.ExtractState(sOut)
	require.NoError(t, err)
	u.RequireSeq(t, lds, 96, 195)
	u.RequireSeq(t, ls, 200, 200)
	u.RequireLastKnownMsg(t, lm, u.Shard(200, 201, 200, toHex(300), toHex(299), nil))
}

func TestProtectedWriteErrors(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	// Start everything
	opts := u.GetTestNATSServerOpts(t)
	s := test.RunServer(&opts)
	defer s.Shutdown()

	u.CreateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 3, time.Hour)

	sOut := Start(makeCfg(s.ClientURL(), "streamout", "teststream", -1, time.Second/5, time.Second/5))
	defer sOut.Stop(true)

	// Empty stream + wrong predecessor seq: blockio.ErrWrongPredecessor
	require.Eventually(t, func() bool {
		err := sOut.ProtectedWrite(context.Background(), 1, "", u.Announcement(2, 2, "", "", nil))
		require.Error(t, err)
		if errors.Is(err, blockio.ErrTemporarilyUnavailable) {
			return false
		}
		require.ErrorIs(t, err, blockio.ErrWrongPredecessor)
		return true
	}, time.Second*5, time.Second/20)

	// Empty stream + wrong predecessor msgid: blockio.ErrWrongPredecessor
	err := sOut.ProtectedWrite(context.Background(), 0, "non-existing-msgid", u.Announcement(1, 1, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Empty stream + wrong predecessor seq + wrong predecessor msgid: blockio.ErrWrongPredecessor
	err = sOut.ProtectedWrite(context.Background(), 1, "non-existing-msgid", u.Announcement(2, 2, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Correct write
	err = sOut.ProtectedWrite(context.Background(), 0, "", u.Announcement(1, 1, "", "", nil))
	require.NoError(t, err)

	// Wrong predecessor seq: blockio.ErrWrongPredecessor
	err = sOut.ProtectedWrite(context.Background(), 2, "1", u.Announcement(2, 2, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Wrong predecessor seq + missing predecessor msgid: blockio.ErrWrongPredecessor
	err = sOut.ProtectedWrite(context.Background(), 2, "", u.Announcement(2, 2, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Wrong predecessor msgid: blockio.ErrWrongPredecessor
	err = sOut.ProtectedWrite(context.Background(), 1, "non-existing-msgid", u.Announcement(2, 2, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Wrong predecessor seq + wrong predecessor msgid: blockio.ErrWrongPredecessor
	err = sOut.ProtectedWrite(context.Background(), 2, "non-existing-msgid", u.Announcement(2, 2, "", "", nil))
	require.ErrorIs(t, err, blockio.ErrWrongPredecessor)

	// Correct write
	err = sOut.ProtectedWrite(context.Background(), 1, "1", u.Announcement(2, 2, "", "", nil))
	require.NoError(t, err)

	// Correct write with no predecessor msgid enforced
	err = sOut.ProtectedWrite(context.Background(), 2, "", u.Announcement(3, 3, "", "", nil))
	require.NoError(t, err)

	// Correct writes
	for i := uint64(4); i <= 100; i++ {
		err = sOut.ProtectedWrite(context.Background(), i-1, strconv.FormatUint(i-1, 10), u.Announcement(i, i, "", "", nil))
		require.NoError(t, err)
	}

	firstSeq, lastSeq := uint64(98), uint64(100)

	for _, predecessorSeq := range []uint64{0, 1, 2, 94, 95, 96, 97, 98, 99, 100, 101, 102, 1000, 1001, 1002} {
		for _, predecessorMsgId := range []string{"", "0", "1", "2", "94", "95", "96", "97", "98", "99", "100", "101", "102", "1000", "1001", "1002"} {
			for _, elem := range []uint64{0, 1, 2, 97, 98, 99, 100, 101, 102, 1000, 1001, 1002} {

				var expectedErr error

				if elem <= lastSeq { // Dedup case
					if elem < firstSeq { // Dedup behavior beyond it's window is undefined
						continue
					}
					if predecessorSeq+1 == elem { // Normal dedup
						expectedErr = nil
					} else { // Dedup into wrong sequence
						expectedErr = blockio.ErrCollision
					}
				} else { // New element
					if predecessorSeq+1 < firstSeq { // Removed position
						expectedErr = blockio.ErrRemovedPosition
					} else if predecessorSeq < lastSeq { // Collision
						expectedErr = blockio.ErrCollision
					} else if predecessorSeq == lastSeq && predecessorMsgId == "" || predecessorMsgId == strconv.FormatUint(predecessorSeq, 10) { // Normal write
						continue
					} else {
						expectedErr = blockio.ErrWrongPredecessor
					}
				}

				msg := u.Announcement(elem, elem, "", "", nil)
				err = sOut.ProtectedWrite(context.Background(), predecessorSeq, predecessorMsgId, msg)
				if expectedErr != nil {
					require.ErrorIs(t, err, expectedErr)
				} else {
					require.NoError(t, err)
				}
			}
		}
	}

	// Make sure writes still work after that
	for i := uint64(101); i <= 200; i++ {
		err = sOut.ProtectedWrite(context.Background(), i-1, strconv.FormatUint(i-1, 10), u.Announcement(i, i, "", "", nil))
		require.NoError(t, err)
	}

	// Making dedup cache window as small as possible
	u.UpdateStream(s.ClientURL(), "teststream", []string{"teststream.*"}, 3, time.Millisecond*100)

	// Restart server to reset dedup cache
	s.Shutdown()
	s = test.RunServer(&opts)
	_ = s

	// Wait until writes start working again
	require.Eventually(t, func() bool {
		err := sOut.ProtectedWrite(context.Background(), 200, "200", u.Announcement(201, 201, "", "", nil))
		if err != nil {
			require.ErrorIs(t, err, blockio.ErrTemporarilyUnavailable)
			return false
		}
		require.NoError(t, err)
		return true
	}, time.Second*5, time.Second/20)

	// Do some writes to get past old dedup window
	for i := uint64(202); i <= 1000; i++ {
		err = sOut.ProtectedWrite(context.Background(), i-1, strconv.FormatUint(i-1, 10), u.Announcement(i, i, "", "", nil))
		require.NoError(t, err)
	}

	// Waiting to until all existing messages fell out from dedup window
	time.Sleep(time.Millisecond * 110)

	// Make sure next writes work normally (including duplicate writes)
	for i := uint64(998); i <= 2000; i++ {
		err = sOut.ProtectedWrite(context.Background(), i-1, strconv.FormatUint(i-1, 10), u.Announcement(i, i, "", "", nil))
		require.NoError(t, err)
	}
}

/*
	TODO: fill remaining misc test-coverage gaps

	go test -v -coverprofile cover.out .
	go tool cover -html cover.out -o cover.html
*/

func toHex(n uint64) string {
	return fmt.Sprintf("%x", n)
}
