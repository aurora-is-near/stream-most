package stream

func Connect(opts *Options) (Interface, error) {
	if opts.ShouldFake {
		if opts.FakeStream != nil {
			return opts.FakeStream, nil
		}

		return createFake(), nil
	}

	return newStream(opts)
}
