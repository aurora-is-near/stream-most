package blockreader

import (
	"fmt"
)

var _ error = (*receiverInterruptedError)(nil)

type receiverInterruptedError struct {
	cause   error
	message string
}

func newReceiverInterruptedError(cause error, messageFormat string, args ...any) *receiverInterruptedError {
	return &receiverInterruptedError{
		cause:   cause,
		message: fmt.Sprintf(messageFormat, args...),
	}
}

func (e *receiverInterruptedError) Error() string {
	if e.cause == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.cause)
}

func (e *receiverInterruptedError) Unwrap() error {
	return e.cause
}
