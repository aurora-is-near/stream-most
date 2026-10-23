package transport

import (
	"net"
	"strings"

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
	if opts.CustomDialer != nil {
		if cd, ok := opts.CustomDialer.(underlyingDialer); ok {
			return cd
		}
	}
	if opts.Dialer != nil {
		return opts.Dialer
	}
	if opts.Timeout != 0 {
		return &net.Dialer{Timeout: opts.Timeout}
	}
	return &net.Dialer{Timeout: nats.DefaultTimeout}
}
