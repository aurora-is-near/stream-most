package shardmask

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type rangeTestCase struct {
	s    string
	l, r int
	err  bool
}

func (tc *rangeTestCase) run(t *testing.T) {
	l, r, err := parseRange(tc.s)
	assert.Equal(t, tc.l, l)
	assert.Equal(t, tc.r, r)
	if tc.err {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

var rangeTestCases = []*rangeTestCase{
	{s: "0", l: 0, r: 0},
	{s: "1", l: 1, r: 1},
	{s: "5324", l: 5324, r: 5324},
	{s: "0-0", l: 0, r: 0},
	{s: "1-50", l: 1, r: 50},
	{s: " 1 - 50 ", l: 1, r: 50},

	{s: "-", err: true},
	{s: ",", err: true},
	{s: "1,", err: true},
	{s: "-1", err: true},
	{s: "1-", err: true},
	{s: "0-0-", err: true},
	{s: "-0-0,", err: true},
	{s: "5-5-5", err: true},
	{s: "5-4", err: true},
	{s: "99999999", err: true},
	{s: "0-99999999", err: true},
	{s: "-1000", err: true},
	{s: "0,1", err: true},
	{s: "o-1", err: true},
	{s: "i", err: true},
}

func TestParseRange(t *testing.T) {
	for _, tc := range rangeTestCases {
		t.Run(fmt.Sprintf("parse range test case '%s'", tc.s), tc.run)
	}
}

type maskTestCase struct {
	s    string
	mask []bool
	err  bool
}

func (tc *maskTestCase) run(t *testing.T) {
	mask, err := ParseShardMask(tc.s)
	assert.Equal(t, tc.mask, mask)
	if tc.err {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

var maskTestCases = []*maskTestCase{
	{s: "", mask: nil},
	{s: "-", mask: []bool{}},
	{s: "0", mask: []bool{true}},
	{s: "2", mask: []bool{false, false, true}},
	{s: "1, 0", mask: []bool{true, true}},
	{s: "2, 0", mask: []bool{true, false, true}},
	{s: "1-3, 2-4, 6", mask: []bool{false, true, true, true, true, false, true}},
	{s: "1, 1, 1, 1", mask: []bool{false, true}},

	{s: " ", err: true},
	{s: " -", err: true},
	{s: "- ", err: true},
	{s: " - ", err: true},
	{s: "o, 1-4", err: true},
	{s: "1-4, i", err: true},
	{s: "0-1 1-2", err: true},
	{s: "0-1,,1-2", err: true},
	{s: "5,,6", err: true},
}

func TestParseShardMask(t *testing.T) {
	for _, tc := range maskTestCases {
		t.Run(fmt.Sprintf("parse shard mask test case '%s'", tc.s), tc.run)
	}
}
