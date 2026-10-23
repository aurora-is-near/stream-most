package transport

import (
	"net"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func processUrlString(url string) []string {
	urls := strings.Split(url, ",")
	var j int
	for _, s := range urls {
		u := strings.TrimSpace(s)
		if len(u) > 0 {
			urls[j] = u
			j++
		}
	}
	return urls[:j]
}

func getUnderlyingDialer(opts *nats.Options) underlyingDialer {
	// Best effort to mimic internal NATS behavior
	if opts.CustomDialer != nil {
		if cd, ok := opts.CustomDialer.(underlyingDialer); ok {
			return cd
		}
	}
	if opts.Dialer != nil {
		return opts.Dialer
	}
	return &net.Dialer{Timeout: getBaseTimeout(opts)}
}

func calculateApproximateConnectionTimeout(opts *nats.Options) time.Duration {
	if len(opts.Servers) > 0 {
		return time.Duration(len(opts.Servers)) * getBaseTimeout(opts)
	}
	return getBaseTimeout(opts)
}

func getBaseTimeout(opts *nats.Options) time.Duration {
	if opts.Timeout == 0 {
		return nats.DefaultTimeout
	}
	return opts.Timeout
}
