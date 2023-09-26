package inputs

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/spf13/cobra"
)

func BackupCmd() (cmd *cobra.Command, inputFactory func() (blockio.Input, error)) {
	cmd = &cobra.Command{
		Use:   "from-backup",
		Short: "Read blocks from .tar.gz backup",
	}

	return cmd, func() (blockio.Input, error) {
		return nil, fmt.Errorf("backup input not implemented")
	}
}
