package drivers

type DriverType string

const (
	NearV3       DriverType = "near_v3"
	NearV3NoSort DriverType = "near_v3_no_sort"
	Nop          DriverType = "nop"
	Jitter       DriverType = "jitter"
)
