package monitor_options

type Options struct {
	ListenAddress         string
	Namespace             string
	Subsystem             string
	StdoutIntervalSeconds uint64
}

func (o *Options) WithDefaults() *Options {
	if o.ListenAddress == "" {
		o.ListenAddress = ":7000"
	}
	return o
}
