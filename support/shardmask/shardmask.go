package shardmask

import (
	"fmt"
	"strconv"
	"strings"
)

const MaxShard = 10000

func ParseShardMask(s string) ([]bool, error) {
	if s == "" {
		return nil, nil
	}

	mask := make([]bool, 0)
	if s == "-" {
		return mask, nil
	}

	for i, rangeRepr := range strings.Split(s, ",") {
		l, r, err := parseRange(rangeRepr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse range #%d: %w", i, err)
		}
		for r >= len(mask) {
			mask = append(mask, false)
		}
		for j := l; j <= r; j++ {
			mask[j] = true
		}
	}
	return mask, nil
}

func parseRange(s string) (int, int, error) {
	ls, rs, both := strings.Cut(s, "-")

	l, err := strconv.ParseUint(strings.TrimSpace(ls), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse left bound of range: %w", err)
	}
	if l > MaxShard {
		return 0, 0, fmt.Errorf("shards greater than #%d are not allowed", MaxShard)
	}

	if !both {
		return int(l), int(l), nil
	}

	r, err := strconv.ParseUint(strings.TrimSpace(rs), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse right bound of range: %w", err)
	}
	if r > MaxShard {
		return 0, 0, fmt.Errorf("shards greater than #%d are not allowed", MaxShard)
	}

	if r < l {
		return 0, 0, fmt.Errorf("right bound of range must be greater or equal to left bound")
	}
	return int(l), int(r), nil
}
