package inputs

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/spf13/cobra"
)

func PipeCmd() (cmd *cobra.Command, inputFactory func() (blockio.Input, error)) {
	cmd = &cobra.Command{
		Use:   "from-pipe",
		Short: "Read blocks from STDIN pipe",
	}

	return cmd, func() (blockio.Input, error) {
		return nil, fmt.Errorf("pipe input not implemented")
	}
}
