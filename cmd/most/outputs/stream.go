package outputs

import (
	"time"

	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/service/outputs/streamoutput"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

func StreamCmd() (cmd *cobra.Command, outputFactory func() (blockio.Output, error)) {
	cmd = &cobra.Command{
		Use:   "to-stream",
		Short: "Write blocks to NATS JetStream",
	}

	cfg := &streamoutput.Config{
		Conn: &streamconnector.Config{
			Nats: &transport.NATSConfig{
				ContextName: "",
				ServerURL:   "",
				Creds:       "",
				LogTag:      "output",
			},
			Stream: &stream.Config{
				Name:        "",
				RequestWait: time.Second * 10,
				WriteWait:   time.Second * 10,
				LogTag:      "output",
			},
		},
		SubjectPattern:        "",
		WriteRetryWait:        jetstream.DefaultPubRetryWait,
		WriteRetryAttempts:    jetstream.DefaultPubRetryAttempts,
		PreserveCustomHeaders: true,
		MaxReconnects:         -1,
		ReconnectDelay:        time.Second,
		StateFetchInterval:    time.Second * 15,
		LogInterval:           time.Minute,
	}

	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.ContextName, "out-nats", "", "Name of the nats context for output stream. If empty - default one will be used")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.ServerURL, "out-url", "", "Override NATS URL for output stream. Comma-separated list is supported")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Nats.Creds, "out-creds", "", "Override path to NATS credentials file for output stream. If empty - won't be used")
	cmd.PersistentFlags().StringVar(&cfg.Conn.Stream.Name, "out-stream", "", "Name of the output stream")
	cmd.PersistentFlags().DurationVar(&cfg.Conn.Stream.RequestWait, "out-request-timeout", time.Second*10, "Read-requests timeout for output stream connection")
	cmd.PersistentFlags().DurationVar(&cfg.Conn.Stream.WriteWait, "out-write-timeout", time.Second*10, "Write-requests timeout for output stream connection")
	cmd.PersistentFlags().StringVar(&cfg.SubjectPattern, "out-subject", "", "Output stream subject pattern for writing. Star symbol ('*') will be replaced with shard number etc. Empty means automatic")
	cmd.PersistentFlags().DurationVar(&cfg.WriteRetryWait, "out-write-retry-wait", jetstream.DefaultPubRetryWait, "Time wait between retries on Publish to output stream if err is NoResponders")
	cmd.PersistentFlags().IntVar(&cfg.WriteRetryAttempts, "out-write-retry-attempts", jetstream.DefaultPubRetryAttempts, "Number of retries on Publish to output stream if err is NoResponders")
	cmd.PersistentFlags().BoolVar(&cfg.PreserveCustomHeaders, "out-preserve-headers", true, "Preserve headers (except nats-specific) in output stream")
	cmd.PersistentFlags().IntVar(&cfg.MaxReconnects, "out-max-reconnects", -1, "Number of output stream reconnections before giving up. Negative value mean infinite")
	cmd.PersistentFlags().DurationVar(&cfg.ReconnectDelay, "out-reconnect-delay", time.Second, "Delay before next output stream reconnection attempt")
	cmd.PersistentFlags().DurationVar(&cfg.StateFetchInterval, "out-state-fetch-interval", time.Second*15, "How frequently output stream state (including it's tip) should be updated")

	util.NoErr(cmd.MarkPersistentFlagRequired("out-stream"))

	return cmd, func() (blockio.Output, error) {
		return streamoutput.Start(cfg, metrics.Dummy), nil
	}
}
