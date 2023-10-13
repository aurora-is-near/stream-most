package formats

type FormatType int

const (
	HeadersOnly FormatType = 0
	NearV2      FormatType = 1
	AuroraV2    FormatType = 2
	NearV3      FormatType = 3
)

var formatNames = map[FormatType]string{
	HeadersOnly: "headers_only",
	NearV2:      "near_v2",
	AuroraV2:    "aurora_v2",
	NearV3:      "near_v3",
}

var formatsByName = map[string]FormatType{}

func init() {
	for f, n := range formatNames {
		formatsByName[n] = f
	}
}

func (f FormatType) String() string {
	if n, ok := formatNames[f]; ok {
		return n
	}
	return "Unknown"
}

func GetFormatByName(name string) (FormatType, bool) {
	t, ok := formatsByName[name]
	return t, ok
}
