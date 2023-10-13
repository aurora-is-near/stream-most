package bridge

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-is-near/stream-most/cmd/most/inputs"
	"github.com/aurora-is-near/stream-most/cmd/most/outputs"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/formats/headers"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/service/verifier"
	"github.com/aurora-is-near/stream-most/support/shardmask"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/spf13/cobra"
)

func BridgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bridge",
		Short: "Reliably copy blocks from source to destination",
	}

	cfg := &bridge.Config{
		MaxReseeks:               -1,
		ReseekDelay:              time.Second,
		ReorderBufferSize:        0,
		InputStartSeq:            0,
		InputEndSeq:              0,
		AnchorOutputSeq:          1,
		AnchorBlock:              nil,
		PreAnchorBlock:           nil,
		AnchorInputSeq:           1,
		LastBlock:                nil,
		EndBlock:                 nil,
		CorruptedBlocksTolerance: -1,
		LowBlocksTolerance:       100000,
		HighBlocksTolerance:      -1,
		WrongBlocksTolerance:     1000,
		NoWriteTolerance:         -1,
	}

	formatStr := cmd.PersistentFlags().String("format", "", "blocks payload format, available values: near_v2, aurora_v2, near_v3")
	headersOnly := cmd.PersistentFlags().Bool("headers-only", false, "Don't parse payload, only headers")
	shardFilterStr := cmd.PersistentFlags().String("shard-filter", "", "Examples: '0,2,5', '1-15,55-200', '0,3,5-55,57'. Empty means no filter, '-' means reject all shards")
	cmd.PersistentFlags().IntVar(&cfg.MaxReseeks, "max-reseeks", cfg.MaxReseeks, "Max attempts to seek again after desync. -1 means infinite")
	cmd.PersistentFlags().DurationVar(&cfg.ReseekDelay, "reseek-delay", cfg.ReseekDelay, "Time to wait on seek retry")
	cmd.PersistentFlags().UintVar(&cfg.ReorderBufferSize, "reorder-buf-size", cfg.ReorderBufferSize, "Max number of elements that can be accumulated and written later in case of mismatch. Making it greater than 1000 will affect performance.")
	cmd.PersistentFlags().Uint64Var(&cfg.InputStartSeq, "input-start-seq", cfg.InputStartSeq, "Lower bound for reader (inclusive)")
	cmd.PersistentFlags().Uint64Var(&cfg.InputEndSeq, "input-end-seq", cfg.InputEndSeq, "Upper bound for reader (exclusive)")
	cmd.PersistentFlags().Uint64Var(&cfg.AnchorOutputSeq, "anchor-output-seq", cfg.AnchorOutputSeq, "In case of empty output stream, bridge will only be allowed to write whatever matches input anchor to this sequence")
	anchorBlock := cmd.PersistentFlags().String("anchor-block", "", "In case of empty output stream, if provided, only this block will be allowed to be written to output anchor sequence. This input anchor has priority over pre-anchor-block and anchor-input-seq")
	preAnchorBlock := cmd.PersistentFlags().String("pre-anchor-block", "", "In case of empty output stream, if provided and not shadowed by anchor-block, only block that follows this one will be allowed to be written to output anchor sequence. This input anchor has priority over anchor-input-seq")
	cmd.PersistentFlags().Uint64Var(&cfg.AnchorInputSeq, "anchor-input-seq", cfg.AnchorInputSeq, "In case of empty output stream, if provided and not shadowed by anchor-block or pre-anchor-block, only input message with such sequence will be allowed to be written to output anchor sequence")
	lastBlock := cmd.PersistentFlags().String("last-block", "", "Upper bound for writer (inclusive)")
	endBlock := cmd.PersistentFlags().String("end-block", "", "Upper bound for writer (exclusive)")
	cmd.PersistentFlags().Int64Var(&cfg.CorruptedBlocksTolerance, "tol-corrupted", cfg.CorruptedBlocksTolerance, "Number of continuous corrupted blocks that can be tolerated. -1 means infinite")
	cmd.PersistentFlags().Int64Var(&cfg.LowBlocksTolerance, "tol-low", cfg.LowBlocksTolerance, "Number of continuous too-low blocks that can be tolerated. -1 means infinite")
	cmd.PersistentFlags().Int64Var(&cfg.HighBlocksTolerance, "tol-high", cfg.HighBlocksTolerance, "Number of continuous too-high blocks that can be tolerated. -1 means infinite")
	cmd.PersistentFlags().Int64Var(&cfg.WrongBlocksTolerance, "tol-wrong", cfg.WrongBlocksTolerance, "Number of continuous corrupted or too-high blocks that can be tolerated. -1 means infinite")
	cmd.PersistentFlags().Int64Var(&cfg.NoWriteTolerance, "tol-no-write", cfg.NoWriteTolerance, "Number of continuous corrupted or too-high or too-low blocks that can be tolerated. -1 means infinite")

	util.NoErr(cmd.MarkPersistentFlagRequired("format"))

	inputProviders := []func() (cmd *cobra.Command, inputFactory func() (blockio.Input, error)){
		inputs.StreamCmd,
		inputs.BackupCmd,
		inputs.PipeCmd,
	}

	outputProviders := []func() (cmd *cobra.Command, outputFactory func() (blockio.Output, error)){
		outputs.StreamCmd,
		outputs.BackupCmd,
		outputs.PipeCmd,
	}

	for _, in := range inputProviders {
		inCmd, inFactory := in()

		for _, out := range outputProviders {
			outCmd, outFactory := out()

			outCmd.RunE = func(cmd *cobra.Command, args []string) error {

				format, ok := formats.GetFormatByName(*formatStr)
				if !ok || format == formats.HeadersOnly {
					return fmt.Errorf("'%s' is not an allowed value for --format", *formatStr)
				}
				if *headersOnly {
					formats.UseFormat(formats.HeadersOnly)
				} else {
					formats.UseFormat(format)
				}
				shardFilter, err := shardmask.ParseShardMask(*shardFilterStr)
				if err != nil {
					return fmt.Errorf("unable to parse shard-filter: %w", err)
				}
				verifier, err := verifier.SequentialForFormat(format, *headersOnly, shardFilter)
				if err != nil {
					return fmt.Errorf("unable to create sequential verifier for given format: %w", err)
				}

				if *anchorBlock != "" {
					if cfg.AnchorBlock, err = headers.ParseMsgID(*anchorBlock); err != nil {
						return fmt.Errorf("unable to parse anchor-block value: %w", err)
					}
				}
				if *preAnchorBlock != "" {
					if cfg.PreAnchorBlock, err = headers.ParseMsgID(*preAnchorBlock); err != nil {
						return fmt.Errorf("unable to parse pre-anchor-block value: %w", err)
					}
				}
				if *lastBlock != "" {
					if cfg.LastBlock, err = headers.ParseMsgID(*lastBlock); err != nil {
						return fmt.Errorf("unable to parse last-block value: %w", err)
					}
				}
				if *endBlock != "" {
					if cfg.EndBlock, err = headers.ParseMsgID(*endBlock); err != nil {
						return fmt.Errorf("unable to parse end-block value: %w", err)
					}
				}

				input, err := inFactory()
				if err != nil {
					return err
				}
				defer input.Stop(true)

				output, err := outFactory()
				if err != nil {
					return err
				}
				defer output.Stop(true)

				b, err := bridge.Start(cfg, verifier, input, output)
				if err != nil {
					return fmt.Errorf("unable to start bridge: %w", err)
				}
				defer b.Stop(true)

				interrupt := make(chan os.Signal, 10)
				signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT)

				select {
				case <-interrupt:
				case <-b.Finished():
				}

				return nil
			}

			inCmd.AddCommand(outCmd)
		}

		cmd.AddCommand(inCmd)
	}

	return cmd
}
