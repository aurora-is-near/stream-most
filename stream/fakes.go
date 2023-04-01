package stream

var defaultFakeProvider func() Interface

func UseFake(provider func() Interface) {
	defaultFakeProvider = provider
}

func createFake() Interface {
	if defaultFakeProvider == nil {
		panic("No default fake for stream.Interface was selected. Please call fakes.UseDefaultOnes()")
	}
	return defaultFakeProvider()
}
