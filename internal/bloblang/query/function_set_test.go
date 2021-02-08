package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionSetWithout(t *testing.T) {
	setOne := AllFunctions
	setTwo := setOne.Without("uuid_v4")

	assert.Contains(t, setOne.List(), "uuid_v4")
	assert.NotContains(t, setTwo.List(), "uuid_v4")

	_, err := setOne.Init("uuid_v4")
	assert.NoError(t, err)

	_, err = setTwo.Init("uuid_v4")
	assert.EqualError(t, err, "unrecognised function 'uuid_v4'")

	_, err = setTwo.Init("timestamp_unix")
	assert.NoError(t, err)
}

func TestFunctionBadName(t *testing.T) {
	testCases := map[string]string{
		"!no":         "function name '!no' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo__bar":    "function name 'foo__bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"-foo-bar":    "function name '-foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar-":    "function name 'foo-bar-' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"":            "function name '' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar":     "function name 'foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar_baz": "function name 'foo-bar_baz' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"FOO":         "function name 'FOO' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foobarbaz":   "",
		"foobarbaz89": "",
		"foo_bar_baz": "",
		"fo1_ba2_ba3": "",
	}

	for k, v := range testCases {
		t.Run(k, func(t *testing.T) {
			setOne := AllFunctions.Without()
			err := setOne.Add(NewFunctionSpec(FunctionCategoryGeneral, k, ""), nil, false)
			if len(v) > 0 {
				assert.EqualError(t, err, v)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
