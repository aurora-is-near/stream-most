package transport

type Options struct {
	Endpoints           []string
	Creds               string
	TimeoutMs           uint
	PingIntervalMs      uint
	MaxPingsOutstanding int
	LogTag              string
	Name                string `json:"-"`
}

func (o *Options) WithDefaults() *Options {
	if o.TimeoutMs == 0 {
		o.TimeoutMs = 10000
	}
	if o.PingIntervalMs == 0 {
		o.PingIntervalMs = 600000
	}
	if o.MaxPingsOutstanding == 0 {
		o.MaxPingsOutstanding = 5
	}
	if len(o.LogTag) == 0 {
		o.LogTag = "unnamed"
	}
	if len(o.Name) == 0 {
		o.Name = "unnamed"
	}
	return o
}
