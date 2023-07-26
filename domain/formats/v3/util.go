package v3

import "unsafe"

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
