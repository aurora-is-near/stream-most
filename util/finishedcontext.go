package util

import "context"

var finishedContext context.Context

func init() {
	var cancel context.CancelFunc
	finishedContext, cancel = context.WithCancel(context.Background())
	cancel()
}

func FinishedContext() context.Context {
	return finishedContext
}
