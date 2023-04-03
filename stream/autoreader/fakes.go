package autoreader

import "github.com/aurora-is-near/stream-most/stream"

type FakeStreamOpener func(options *stream.Options) stream.Interface

var defaultFakeStreamOpener FakeStreamOpener

func UseFakeStream(opener FakeStreamOpener) {
	defaultFakeStreamOpener = opener
}
