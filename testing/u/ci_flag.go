package u

import "flag"

var isCI = flag.Bool("ci", false, "")

func IsCI() bool {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *isCI
}
