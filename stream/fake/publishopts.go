package fake

import (
	"reflect"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func applyPublishOpts(header nats.Header, opts ...jetstream.PublishOpt) nats.Header {
	if len(opts) == 0 {
		return header
	}

	optFuncType := reflect.TypeOf(opts[0])
	optStructType := optFuncType.In(0).Elem()
	optStruct := reflect.New(optStructType)
	for _, opt := range opts {
		reflect.ValueOf(opt).Call([]reflect.Value{optStruct})
	}

	if id := optStruct.Elem().FieldByName("id").String(); len(id) > 0 {
		header.Set(jetstream.MsgIDHeader, id)
	}
	if lastMsgID := optStruct.Elem().FieldByName("lastMsgID").String(); len(lastMsgID) > 0 {
		header.Set(jetstream.ExpectedLastMsgIDHeader, lastMsgID)
	}
	if stream := optStruct.Elem().FieldByName("stream").String(); len(stream) > 0 {
		header.Set(jetstream.ExpectedStreamHeader, stream)
	}
	if lastSeqPtr := optStruct.Elem().FieldByName("lastSeq"); !lastSeqPtr.IsNil() {
		lastSeq := strconv.FormatUint(lastSeqPtr.Elem().Uint(), 10)
		header.Set(jetstream.ExpectedLastSeqHeader, lastSeq)
	}
	if lastSubjectSeqPtr := optStruct.Elem().FieldByName("lastSubjectSeq"); !lastSubjectSeqPtr.IsNil() {
		lastSubjectSeq := strconv.FormatUint(lastSubjectSeqPtr.Elem().Uint(), 10)
		header.Set(jetstream.ExpectedLastSubjSeqHeader, lastSubjectSeq)
	}

	return header
}
