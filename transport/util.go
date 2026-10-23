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
		copyDialer := *opts.Dialer
		copyDialer.Timeout = perHostTimeout(copyDialer.Timeout, len(opts.Servers))
		return &copyDialer
	}
	if opts.Timeout != 0 {
		return &net.Dialer{Timeout: perHostTimeout(opts.Timeout, len(opts.Servers))}
	}
	return &net.Dialer{Timeout: perHostTimeout(nats.DefaultTimeout, len(opts.Servers))}
}

func perHostTimeout(timeout time.Duration, hostsCount int) time.Duration {
	if hostsCount > 0 {
		return timeout / time.Duration(hostsCount)
	}
	return timeout
}
