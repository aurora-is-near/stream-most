package u

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

func GetFreePorts(n int) []int {
	listeners := []*net.TCPListener{}
	defer func() {
		for _, listener := range listeners {
			if err := listener.Close(); err != nil {
				panic(fmt.Errorf("can't release obtained free port: %w", err))
			}
		}
	}()

	ports := make([]int, n)
	for i := 0; i < n; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			panic(fmt.Errorf("can't lookup free port: %w", err))
		}
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			panic(fmt.Errorf("can't obtain free port: %w", err))
		}
		listeners = append(listeners, listener)
		ports[i] = listener.Addr().(*net.TCPAddr).Port
	}

	return ports
}

func GetTestNATSServerOpts(t *testing.T) server.Options {
	srvOpts := test.DefaultTestOptions
	srvOpts.Port = GetFreePorts(1)[0]
	srvOpts.JetStream = true
	srvOpts.StoreDir = t.TempDir()
	return srvOpts
}

func ConnectTestNATS(natsUrl string, connName string) (*transport.NatsConnection, jetstream.JetStream) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	natsCfg := &transport.NATSConfig{
		OverrideURL:    natsUrl,
		LogTag:         connName,
		Options:        transport.RecommendedNatsOptions(),
		OverrideLogger: logger,
	}
	nc, err := transport.ConnectNATS(natsCfg)
	if err != nil {
		panic(fmt.Errorf("unable to connect to nats (%s) for %s: %w", natsUrl, connName, err))
	}

	js, err := jetstream.New(nc.Conn())
	if err != nil {
		panic(fmt.Errorf("unable to connect to jetstream (%s) for %s: %w", natsUrl, connName, err))
	}

	return nc, js
}

func CreateStream(natsUrl string, streamName string, subjects []string, maxMsgs int64, dedup time.Duration) {
	nc, js := ConnectTestNATS(natsUrl, "stream-creation")
	defer nc.Drain()

	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          subjects,
		Retention:         jetstream.LimitsPolicy,
		MaxConsumers:      -1,
		MaxMsgs:           maxMsgs,
		MaxBytes:          -1,
		Discard:           jetstream.DiscardOld,
		Storage:           jetstream.FileStorage,
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        -1,
		Replicas:          1,
		Duplicates:        dedup,
		AllowDirect:       true,
	})
	if err != nil {
		panic(fmt.Errorf("unable to create jetstream '%s' on nats '%s': %v", streamName, natsUrl, err))
	}
}

func UpdateStream(natsUrl string, streamName string, subjects []string, maxMsgs int64, dedup time.Duration) {
	nc, js := ConnectTestNATS(natsUrl, "stream-updating")
	defer nc.Drain()

	_, err := js.UpdateStream(context.Background(), jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          subjects,
		Retention:         jetstream.LimitsPolicy,
		MaxConsumers:      -1,
		MaxMsgs:           maxMsgs,
		MaxBytes:          -1,
		Discard:           jetstream.DiscardOld,
		Storage:           jetstream.FileStorage,
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        -1,
		Replicas:          1,
		Duplicates:        dedup,
		AllowDirect:       true,
	})
	if err != nil {
		panic(fmt.Errorf("unable to update jetstream '%s' on nats '%s': %v", streamName, natsUrl, err))
	}
}

func WriteBlocks(natsUrl string, streamName string, subjectPattern string, preserveSequence bool, msgs ...*messages.BlockMessage) {
	nc, js := ConnectTestNATS(natsUrl, "blocks-writing")
	defer nc.Drain()

	for _, msg := range msgs {
		var subject string
		switch msg.Block.GetBlockType() {
		case blocks.Announcement:
			subject = strings.ReplaceAll(subjectPattern, "*", "header")
		case blocks.Shard:
			subject = strings.ReplaceAll(subjectPattern, "*", strconv.FormatUint(msg.Block.GetShardID(), 10))
		default:
			subject = strings.ReplaceAll(subjectPattern, "*", "unknown")
		}

		msgID := blocks.ConstructMsgID(msg.Block)
		opts := []jetstream.PublishOpt{
			jetstream.WithExpectStream(streamName),
			jetstream.WithMsgID(msgID),
		}
		if preserveSequence && msg.Msg.GetSequence() > 0 {
			opts = append(opts, jetstream.WithExpectLastSequence(msg.Msg.GetSequence()-1))
		}

		ack, err := js.Publish(context.Background(), subject, msg.Msg.GetData(), opts...)
		if err != nil {
			panic(fmt.Errorf("can't write block with msgid='%s': %w", msgID, err))
		}
		if preserveSequence && msg.Msg.GetSequence() > 0 && ack.Sequence != msg.Msg.GetSequence() {
			panic(fmt.Errorf("block with msgid='%s' was expected to be written on seq=%d, but got written on seq=%d", msgID, msg.Msg.GetSequence(), ack.Sequence))
		}
	}
}

func DisplayStreamBlocks(natsUrl string, streamName string, from uint64, to uint64, verbose bool) {
	nc, js := ConnectTestNATS(natsUrl, "blockstream-displaying")
	defer nc.Drain()

	s, err := js.Stream(context.Background(), streamName)
	if err != nil {
		panic(fmt.Errorf("unable to get info for stream '%s' (natsUrl='%s'): %w", streamName, natsUrl, err))
	}
	st := s.CachedInfo().State
	log.Printf("Stream '%s' (natsUrl='%s'): msgs=%d, bytes=%d, firstSeq=%d, lastSeq=%d", streamName, natsUrl, st.Msgs, st.Bytes, st.FirstSeq, st.LastSeq)

	if from < st.FirstSeq {
		log.Printf("  ->  Msgs on seqs [%d;%d] are missing", from, st.FirstSeq-1)
		from = st.FirstSeq
	}
	for i := from; i <= to && i <= st.LastSeq; i++ {
		msg, err := s.GetMsg(context.Background(), i)
		if err != nil {
			panic(fmt.Errorf("unable to get msg with seq=%d from stream '%s' (natsUrl='%s'): %w", i, streamName, natsUrl, err))
		}
		if verbose {
			panic("verbose display not implemented")
		} else {
			log.Printf("  ->  seq=%d, subject='%s', msgid='%s'", i, msg.Subject, msg.Header.Get(jetstream.MsgIDHeader))
		}
	}
	if to > st.LastSeq {
		log.Printf("  ->  Msgs on seqs [%d;%d] are missing", st.LastSeq+1, to)
	}
}

func GetStreamBlocks(natsUrl string, streamName string, from uint64, to uint64) map[uint64]blockio.Msg {
	nc, js := ConnectTestNATS(natsUrl, "blockstream-reading")
	defer nc.Drain()

	s, err := js.Stream(context.Background(), streamName)
	if err != nil {
		panic(fmt.Errorf("unable to get info for stream '%s' (natsUrl='%s'): %w", streamName, natsUrl, err))
	}
	st := s.CachedInfo().State

	blockdecode.EnsureDecodersRunning()
	res := make(map[uint64]blockio.Msg)
	for i := from; i <= to && i <= st.LastSeq; i++ {
		if i < st.FirstSeq {
			res[i] = nil
			continue
		}
		msg, err := s.GetMsg(context.Background(), i)
		if err != nil {
			panic(fmt.Errorf("unable to get msg with seq=%d from stream '%s' (natsUrl='%s'): %w", i, streamName, natsUrl, err))
		}
		bmsg, _ := blockdecode.ScheduleBlockDecoding(context.Background(), &messages.RawStreamMessage{RawStreamMsg: msg})
		res[i] = bmsg
	}
	return res
}
