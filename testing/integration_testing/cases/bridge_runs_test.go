package cases

import (
	"errors"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/testing/integration_testing/runner"
	"testing"
	"time"
)

// TestBridgeRuns is a default test that checks that bridge is bootable
func TestBridgeRuns(t *testing.T) {
	err := runner.NewRunner(runner.Bridge,
		runner.WithDeadline(time.Now().Add(1*time.Second)),
		runner.WithInputStream(fake.NewStream()),
		runner.WithOutputStream(fake.NewStream()),
	).Run()

	// In case of empty input stream, we must get an error
	if err != nil {
		if !errors.Is(err, stream_seek.ErrNotFound) {
			t.Error(err)
		}
	}
}
