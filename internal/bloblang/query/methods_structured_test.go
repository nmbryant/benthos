package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMethodImmutability(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target interface{}
		args   []interface{}
		exp    interface{}
	}{
		{
			name:   "merge arrays",
			method: "merge",
			target: []interface{}{"foo", "bar"},
			args: []interface{}{
				[]interface{}{"baz", "buz"},
			},
			exp: []interface{}{"foo", "bar", "baz", "buz"},
		},
		{
			name:   "merge into an array",
			method: "merge",
			target: []interface{}{"foo", "bar"},
			args: []interface{}{
				map[string]interface{}{"baz": "buz"},
			},
			exp: []interface{}{"foo", "bar", map[string]interface{}{"baz": "buz"}},
		},
		{
			name:   "merge objects",
			method: "merge",
			target: map[string]interface{}{"foo": "bar"},
			args: []interface{}{
				map[string]interface{}{"baz": "buz"},
			},
			exp: map[string]interface{}{
				"foo": "bar",
				"baz": "buz",
			},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := IClone(test.target)
			argsClone := IClone(test.args).([]interface{})

			fn, err := InitMethod(test.method, NewLiteralFunction(targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(FunctionContext{
				Maps:     map[string]Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}
