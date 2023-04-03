package fakes

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/autoreader"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

/*
	While doing complex testing, we might not know what implementation of fakes to use.
	This file registers all the default fakes for tests to use.
*/

// UseDefaultOnes registers basic fakes that do their job.
// You might want to register a bit trickier fakes instead, e.g. for calls mocking
func UseDefaultOnes() {
	stream.UseFake(fake.NewFakeStream)
	reader.UseFake(fake.StartReader)
	autoreader.UseFakeStream(fake.NewFakeStreamWithOptions)
}
