package outputter

type Config struct {
	NatsEndpoints []string
	NatsCredsPath string
	StreamName    string
	Subject       string
	LogTag        string
	StartHeight   uint64
}
