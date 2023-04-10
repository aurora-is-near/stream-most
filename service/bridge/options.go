package bridge

type Options struct {
	InputStartSequence uint64
	InputEndSequence   uint64

	// How many parses should fail in a row so that we exit the program?
	ParseTolerance uint64
}
