package bridge

import (
	"time"

	"github.com/aurora-is-near/stream-most/cmd/most/inputs"
	"github.com/aurora-is-near/stream-most/cmd/most/outputs"
	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/spf13/cobra"
)

func BridgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bridge",
		Short: "Reliably copy blocks from source to destination",
	}

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

				time.Sleep(time.Second * 10)

				return nil
			}

			inCmd.AddCommand(outCmd)
		}

		cmd.AddCommand(inCmd)
	}

	return cmd
}
