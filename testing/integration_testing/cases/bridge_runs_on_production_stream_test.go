package cases

import (
	"context"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/monitor"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/integration_testing/runner"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/sirupsen/logrus"
	"reflect"
	"testing"
)

func TestBridgeRunsOnProductionStream(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	formats.UseFormat(formats.NearV3)

	driver := drivers.Infer(drivers.NearV3, nil, fake.NewFakeStream())
	logrus.Debug("Driver inferred: ", reflect.TypeOf(driver).String())

	inputStream, err := u.DefaultProductionStream()
	if err != nil {
		t.Error(err)
	}

	logrus.Debug("Getting input stream info...")
	info, _, _ := inputStream.GetInfo(0)
	logrus.Debug("Input stream info received")

	go monitor.NewMetricsServer(&monitor_options.Options{
		ListenAddress:         "localhost:9999",
		Namespace:             "lol",
		Subsystem:             "lol",
		StdoutIntervalSeconds: 10,
	}).Serve(context.Background(), true)

	fakeOutputStream := fake.NewStream()

	err = runner.NewRunner(runner.Bridge,
		//runner.WithDeadline(time.Now().Add(1*time.Second)),
		runner.WithBridgeOptions(&bridge.Options{
			InputStartSequence: info.State.FirstSeq + 10000,
			InputEndSequence:   info.State.LastSeq,
			ParseTolerance:     1000,
		}),
		runner.WithReaderOptions((&reader.Options{
			WrongSeqToleranceWindow: 1000,
		}).WithDefaults()),
		runner.WithInputStream(inputStream),
		runner.WithOutputStream(fakeOutputStream),
		runner.WithDriver(driver),
		runner.WithWritesLimit(1),
	).Run()

	// In case of empty input stream, we must get an error
	if err != nil {
		t.Error(err)
	}

	// Output stream must contain more than one message
	if len(fakeOutputStream.GetArray()) == 0 {
		t.Error("Wrong number of messages in output stream, expected more than 0")
	}
}
