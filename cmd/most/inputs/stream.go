package inputs

import (
	"time"

	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/inputs/streaminput"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/spf13/cobra"
)

func StreamCmd() (cmd *cobra.Command, inputFactory func() (blockio.Input, error)) {
	cmd = &cobra.Command{
		Use:   "from-stream",
		Short: "Read blocks from NATS JetStream",
	}

	cfg := &streaminput.Config{
		Stream: &stream.Options{
			Nats: &transport.NATSConfig{
				ContextName:   "",
				OverrideURL:   "",
				OverrideCreds: "",
				LogTag:        "input",
				Options:       transport.RecommendedNatsOptions(),
			},
			Stream:      "",
			RequestWait: time.Second * 10,
			WriteWait:   time.Second * 10,
		},
		FilterSubjects:     []string{},
		StartSeq:           0,
		EndSeq:             0,
		MaxSilence:         time.Second * 10,
		BufferSize:         1024,
		MaxReconnects:      -1,
		ReconnectDelay:     time.Second,
		StateFetchInterval: time.Second * 15,
	}

	cmd.PersistentFlags().StringVar(&cfg.Stream.Nats.ContextName, "in-nats", "", "Name of the nats context for input stream. If empty - default one will be used")
	cmd.PersistentFlags().StringVar(&cfg.Stream.Nats.OverrideURL, "in-url", "", "Override NATS URL for input stream. Comma-separated list is supported")
	cmd.PersistentFlags().StringVar(&cfg.Stream.Nats.OverrideCreds, "in-creds", "", "Override path to NATS credentials file for input stream. If empty - won't be used")
	cmd.PersistentFlags().StringVar(&cfg.Stream.Stream, "in-stream", "", "Name of the input stream")
	cmd.PersistentFlags().DurationVar(&cfg.Stream.RequestWait, "in-request-timeout", time.Second*10, "Read-requests timeout for input stream connection")
	cmd.PersistentFlags().StringSliceVar(&cfg.FilterSubjects, "in-subjects", []string{}, "Subject filter-array for input stream (comma separated). If empty - whole stream will be read")
	cmd.PersistentFlags().Uint64Var(&cfg.StartSeq, "in-start-seq", 0, "Lower bound of allowed sequence range of input stream to read from (inlcusive)")
	cmd.PersistentFlags().Uint64Var(&cfg.EndSeq, "in-end-seq", 0, "Upper bound of allowed sequence range of input stream to read from (exclusive). 0 means no bound")
	cmd.PersistentFlags().DurationVar(&cfg.MaxSilence, "in-max-silence", time.Second*10, "Max duration of silence from input stream reader that won't lead to dead consumer suspicion checks")
	cmd.PersistentFlags().UintVar(&cfg.BufferSize, "in-buf-size", 1024, "Number of messages buffered by input stream reader")
	cmd.PersistentFlags().IntVar(&cfg.MaxReconnects, "in-max-reconnects", -1, "Number of input stream reconnections before giving up. Negative value mean infinite")
	cmd.PersistentFlags().DurationVar(&cfg.ReconnectDelay, "in-reconnect-delay", time.Second, "Delay before next input stream reconnection attempt")
	cmd.PersistentFlags().DurationVar(&cfg.StateFetchInterval, "in-state-fetch-interval", time.Second*15, "How frequently input stream state (including it's tip) should be updated")

	util.NoErr(cmd.MarkPersistentFlagRequired("in-stream"))

	return cmd, func() (blockio.Input, error) {
		return streaminput.Start(cfg), nil
	}
}
