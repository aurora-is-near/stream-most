package inputter

type Config struct {
	NatsEndpoints []string
	NatsCredsPath string
	StreamName    string
	MinSeq        uint64
	LogTag        string
	TrustHeaders  bool
}
