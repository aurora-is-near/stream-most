package multistreambridge

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/multistreambridge/inputter"
	"github.com/aurora-is-near/stream-most/multistreambridge/jobrestarter"
	"github.com/aurora-is-near/stream-most/multistreambridge/outputter"
)

func Run(ctx context.Context, cfg *Config) error {
	switch strings.ToLower(cfg.BlocksFormat) {
	case "aurora":
		formats.UseFormat(formats.AuroraV2)
	case "near":
		formats.UseFormat(formats.NearV2)
	default:
		return fmt.Errorf("unsupported block format '%s', required 'aurora' or 'near'", cfg.BlocksFormat)
	}

	out, err := outputter.NewOutputter(cfg.Output, uint(len(cfg.Inputs)))
	if err != nil {
		return fmt.Errorf("unable to initialize outputter '%s': %w", cfg.Output.LogTag, err)
	}
	outRestarter := jobrestarter.StartJobRestarter(ctx, out, time.Second*3)
	defer outRestarter.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("bridge is stopping"))

	for _, inCfg := range cfg.Inputs {
		in, err := inputter.NewInputter(inCfg, out)
		if err != nil {
			return fmt.Errorf("unable to initalize inputter '%s': %w", inCfg.LogTag, err)
		}
		inRestarter := jobrestarter.StartJobRestarter(ctx, in, time.Second*3)
		defer inRestarter.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("bridge is stopping"))
	}

	<-ctx.Done()

	return nil
}
