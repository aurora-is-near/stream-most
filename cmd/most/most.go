package main

import (
	"os"

	"github.com/aurora-is-near/stream-most/cmd/most/bridge"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "most",
	Short:        "Most that you can do with your streams",
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(bridge.BridgeCmd())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		logrus.Errorf("Program finished with error: %v", err)
		os.Exit(1)
	}
}
