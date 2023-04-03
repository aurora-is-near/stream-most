package monitoring

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func Serve() {
	http.Handle("/metrics", promhttp.Handler())
	_ = http.ListenAndServe(":7000", nil)
}
