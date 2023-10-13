package outputs

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/spf13/cobra"
)

func BackupCmd() (cmd *cobra.Command, outputFactory func() (blockio.Output, error)) {
	cmd = &cobra.Command{
		Use:   "to-backup",
		Short: "Write blocks to .tar.gz backup",
	}

	return cmd, func() (blockio.Output, error) {
		return nil, fmt.Errorf("backup output not implemented")
	}
}
