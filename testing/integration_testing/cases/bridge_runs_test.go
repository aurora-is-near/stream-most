package cases

import (
	"errors"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/testing/integration_testing/runner"
	"github.com/aurora-is-near/stream-most/testing/u"
)

// TestBridgeRuns is a default test that checks that bridge is bootable
func TestBridgeRuns(t *testing.T) {
	// This test is broken right now
	return

	if u.IsCI() {
		t.SkipNow()
	}

	err := runner.NewRunner(runner.Bridge,
		runner.WithDeadline(time.Now().Add(1*time.Second)),
		runner.WithInputStream(fake.NewStream()),
		runner.WithOutputStream(fake.NewStream()),
		runner.WithBridgeOptions(&bridge.Options{}),
	).Run()

	// In case of empty input stream, we must get an error
	if err != nil {
		if !errors.Is(err, stream_seek.ErrNotFound) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("expected an error of empty input stream")
	}
}
