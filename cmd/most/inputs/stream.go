package inputs

import (
	"time"

	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/inputs/streaminput"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
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
		Conn: &streamconnector.Config{
			Nats: &transport.NATSConfig{
				ContextName:   "",
				OverrideURL:   "",
				OverrideCreds: "",
				LogTag:        "input",
				Options:       transport.RecommendedNatsOptions(),
			},
			Stream: &stream.Config{
				Name:        "",
				RequestWait: time.Second * 10,
				WriteWait:   time.Second * 10,
				LogTag:      "input",
			},
		},
		FilterSubjects:     []string{},
		MaxSilence:         time.Second * 10,
		BufferSize:         1024,
		MaxReconnects:      -1,
		ReconnectDelay:     time.Second,
		StateFetchInterval: time.Second * 15,
		LogInterval:        time.Minute,
	}

	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.ContextName, "in-nats", "", "Name of the nats context for input stream. If empty - default one will be used")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.OverrideURL, "in-url", "", "Override NATS URL for input stream. Comma-separated list is supported")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.OverrideCreds, "in-creds", "", "Override path to NATS credentials file for input stream. If empty - won't be used")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Stream.Name, "in-stream", "", "Name of the input stream")
	cmd.PersistentFlags().DurationVar(&cfg.Conn.Stream.RequestWait, "in-request-timeout", time.Second*10, "Read-requests timeout for input stream connection")
	cmd.PersistentFlags().StringSliceVar(&cfg.FilterSubjects, "in-subjects", []string{}, "Subject filter-array for input stream (comma separated). If empty - whole stream will be read")
	cmd.PersistentFlags().DurationVar(&cfg.MaxSilence, "in-max-silence", time.Second*10, "Max duration of silence from input stream reader that won't lead to dead consumer suspicion checks")
	cmd.PersistentFlags().UintVar(&cfg.BufferSize, "in-buf-size", 1024, "Number of messages buffered by input stream reader")
	cmd.PersistentFlags().IntVar(&cfg.MaxReconnects, "in-max-reconnects", -1, "Number of input stream reconnections before giving up. Negative value mean infinite")
	cmd.PersistentFlags().DurationVar(&cfg.ReconnectDelay, "in-reconnect-delay", time.Second, "Delay before next input stream reconnection attempt")
	cmd.PersistentFlags().DurationVar(&cfg.StateFetchInterval, "in-state-fetch-interval", time.Second*15, "How frequently input stream state (including it's tip) should be updated")

	util.NoErr(cmd.MarkPersistentFlagRequired("in-stream"))

	return cmd, func() (blockio.Input, error) {
		return streaminput.Start(cfg, metrics.Dummy), nil
	}
}
