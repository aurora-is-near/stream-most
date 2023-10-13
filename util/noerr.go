package util

func NoErr(err error) {
	if err != nil {
		panic(err)
	}
}
