package cases

import (
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/testing/integration_testing/runner"
	"github.com/aurora-is-near/stream-most/testing/u"
	"testing"
)

func TestBridgeRunsOnProductionStream(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	driver := drivers.Infer(drivers.NearV3, nil, fake.NewFakeStream())

	inputStream, err := u.DefaultProductionStream()
	if err != nil {
		t.Error(err)
	}

	err = runner.NewRunner(runner.Bridge,
		//runner.WithDeadline(time.Now().Add(1*time.Second)),
		runner.WithBridgeOptions(&bridge.Options{
			InputStartSequence: 0,
			InputEndSequence:   0,
			ParseTolerance:     0,
		}),
		runner.WithInputStream(inputStream),
		runner.WithOutputStream(fake.NewStream()),
		runner.WithDriver(driver),
	).Run()

	// In case of empty input stream, we must get an error
	if err != nil {
		t.Error(err)
	}
}
