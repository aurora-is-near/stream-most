package when_interrupted

import (
	"os"
	"os/signal"
	"syscall"
)

var listeners []func()

func Call(f func()) {
	listeners = append(listeners, f)
}

func init() {
	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGINT, syscall.SIGUSR1)
	go func() {
		<-interrupt
		for _, f := range listeners {
			f()
		}
	}()
}
