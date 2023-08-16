package cases

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/integration_testing/runner"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/sirupsen/logrus"
)

func TestBridgeRunsOnProductionStream(t *testing.T) {
	if u.IsCI() {
		t.SkipNow()
	}

	logrus.SetLevel(logrus.DebugLevel)
	formats.UseFormat(formats.NearV3)
	fakes.UseDefaultOnes()

	driver := drivers.Infer(drivers.NearV3, nil, fake.NewFakeStream())
	logrus.Debug("Driver inferred: ", reflect.TypeOf(driver).String())

	inputStream, err := u.DefaultProductionStream()
	if err != nil {
		t.Fatal(err)
	}

	logrus.Debug("Getting input stream info...")
	info, _ := inputStream.GetInfo(context.Background())
	logrus.Debug("Input stream info received")

	go monitor.NewMetricsServer(&monitor_options.Options{
		ListenAddress:         "localhost:9999",
		Namespace:             "testing",
		Subsystem:             "testing",
		StdoutIntervalSeconds: 10,
	}).Serve(context.Background(), true)

	fakeOutputStream := fake.NewStream()

	err = runner.NewRunner(runner.Bridge,
		runner.WithDeadline(time.Now().Add(10*time.Second)),
		runner.WithBridgeOptions(&bridge.Options{
			InputStartSequence: info.State.FirstSeq,
			InputEndSequence:   info.State.LastSeq,
			ParseTolerance:     1000,
		}),
		runner.WithReaderOptions((&reader.Options{}).WithDefaults()),
		runner.WithInputStream(inputStream),
		runner.WithOutputStream(fakeOutputStream),
		runner.WithDriver(driver),
		runner.WithWritesLimit(1),
	).Run()

	if err != nil {
		t.Fatal(err)
	}

	// Output stream must contain more than one message
	if len(fakeOutputStream.GetArray()) == 0 {
		t.Error("Wrong number of messages in output stream, expected more than 0")
	}

	// Validate the output stream
	err = runner.NewRunner(runner.Validator,
		runner.WithDeadline(time.Now().Add(5*time.Second)),
		runner.WithReaderOptions((&reader.Options{}).WithDefaults()),
		runner.WithValidatorOptions(0, 0),
		runner.WithInputStream(fakeOutputStream),
	).Run()
	logrus.Info("Validator finished")

	if err != nil {
		t.Fatal(err)
	}
}
