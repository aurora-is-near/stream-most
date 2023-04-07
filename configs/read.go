package configs

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

func ReadTo(defaultConfigFile string, config interface{}) {
	if len(os.Args) > 1 {
		defaultConfigFile = os.Args[1]
		logrus.Infof("Reading config from %s", defaultConfigFile)
	}

	viper.SetConfigFile(defaultConfigFile)
	viper.AddConfigPath(".")
	viper.SetConfigType("json")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(config); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error parsing config file: %s\n", err)
		os.Exit(1)
	}
}
