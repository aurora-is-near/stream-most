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
	"time"
)

func TestBridgeRunsOnTwoStreams(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	formats.UseFormat(formats.NearV3)

	driver := drivers.Infer(drivers.NearV3, nil, fake.NewFakeStream())
	logrus.Debug("Driver inferred: ", reflect.TypeOf(driver).String())

	inputStream, err := u.DefaultProductionStream()
	if err != nil {
		t.Fatal(err)
	}
	info, _, _ := inputStream.GetInfo(0)

	outputStream, err := u.DefaultLocalStream()
	if err != nil {
		t.Fatal(err)
	}

	err = outputStream.Js().PurgeStream("testing_stream")
	if err != nil {
		t.Fatal(err)
	}

	monitoring := monitor.NewMetricsServer(&monitor_options.Options{
		ListenAddress:         "localhost:9999",
		Namespace:             "testing",
		Subsystem:             "testing",
		StdoutIntervalSeconds: 10,
	})
	go monitoring.Serve(context.Background(), true)

	err = runner.NewRunner(runner.Bridge,
		//runner.WithDeadline(time.Now().Add(1*time.Second)),
		runner.WithBridgeOptions(&bridge.Options{
			InputStartSequence: info.State.FirstSeq,
			InputEndSequence:   info.State.LastSeq,
			ParseTolerance:     1000,
		}),
		runner.WithReaderOptions((&reader.Options{
			WrongSeqToleranceWindow: 1000,
		}).WithDefaults()),
		runner.WithInputStream(inputStream),
		runner.WithOutputStream(outputStream),
		runner.WithDriver(driver),
		runner.WithWritesLimit(100),
	).Run()

	if err != nil {
		t.Error(err)
	}

	// Validate the output stream
	err = runner.NewRunner(runner.Validator,
		runner.WithDeadline(time.Now().Add(5*time.Second)),
		runner.WithReaderOptions((&reader.Options{
			WrongSeqToleranceWindow: 1000,
		}).WithDefaults()),
		runner.WithValidatorOptions(0, 0),
		runner.WithInputStream(outputStream),
	).Run()
	logrus.Info("Validator finished")

	if err != nil {
		t.Error(err)
	}

	monitoring.Spew()
}
