package outputs

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/spf13/cobra"
)

func PipeCmd() (cmd *cobra.Command, outputFactory func() (blockio.Output, error)) {
	cmd = &cobra.Command{
		Use:   "to-pipe",
		Short: "Write blocks to STDOUT pipe",
	}

	return cmd, func() (blockio.Output, error) {
		return nil, fmt.Errorf("pipe output not implemented")
	}
}
