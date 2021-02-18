package mapping

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignments(t *testing.T) {
	type part struct {
		Content string
		Meta    map[string]string
	}

	metaKey := func(k string) *string {
		return &k
	}

	initFunc := func(name string, args ...interface{}) query.Function {
		t.Helper()
		fn, err := query.InitFunction(name, args...)
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping *Executor
		output  *part
		err     error
	}{
		"simple json map": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("bar")),
				NewStatement(nil, NewJSONAssignment("bar"), query.NewLiteralFunction("test2")),
				NewStatement(nil, NewJSONAssignment("zed"), query.NewLiteralFunction(query.Delete(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `{"bar":"test2","foo":"test1"}`},
		},
		"map to root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction("bar")),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `bar`},
		},
		"delete root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction(query.Delete(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: nil,
		},
		"no mapping to root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction(query.Nothing(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `{"bar":"test1","zed":"gone"}`},
		},
		"variable error DNE": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewVarFunction("doesnt exist")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to execute mapping query at line 0: variable 'doesnt exist' undefined"),
		},
		"variable assignment": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewVarAssignment("foo"), query.NewLiteralFunction("does exist")),
				NewStatement(nil, NewJSONAssignment("foo"), query.NewVarFunction("foo")),
			),
			input:  []part{{Content: `{}`}},
			output: &part{Content: `{"foo":"does exist"}`},
		},
		"meta query error": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), initFunc("meta", "foo")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to execute mapping query at line 0: metadata value 'foo' not found"),
		},
		"meta assignment": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(metaKey("foo")), query.NewLiteralFunction("exists now")),
			),
			input: []part{{Content: `{}`}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "exists now",
				},
			},
		},
		"meta deletion": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(metaKey("and")), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"ignore": "me",
					"and":    "delete me",
				},
			}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"ignore": "me",
				},
			},
		},
		"meta set all error wrong type": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction("foo")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to assign query result at line 0: setting root meta object requires object value, received: string"),
		},
		"meta set all": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction(map[string]interface{}{
					"new1": "value1",
					"new2": "value2",
				})),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "first",
					"bar": "second",
				},
			}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"new1": "value1",
					"new2": "value2",
				},
			},
		},
		"meta delete all": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "first",
					"bar": "second",
				},
			}},
			output: &part{Content: `{}`},
		},
		"invalid json message": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("bar"), query.NewLiteralFunction("test2")),
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("bar")),
				NewStatement(nil, NewJSONAssignment("zed"), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{Content: `{@#$ not valid json`}},
			err:   errors.New("failed to execute mapping query at line 0: unable to reference message as structured (with 'this'): parse as json: invalid character '@' looking for beginning of object key string"),
		},
		"json parse empty message": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("bar"), query.NewLiteralFunction("test2")),
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("bar")),
				NewStatement(nil, NewJSONAssignment("zed"), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{Content: ``}},
			err:   errors.New("failed to execute mapping query at line 0: unable to reference message as structured (with 'this'): message is empty"),
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.New(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				if len(p.Content) == 0 {
					part = message.NewPart(nil)
				}
				for k, v := range p.Meta {
					part.Metadata().Set(k, v)
				}
				msg.Append(part)
			}

			resPart, err := test.mapping.MapPart(test.index, msg)
			if test.err != nil {
				assert.EqualError(t, err, test.err.Error())
				return
			}

			require.NoError(t, err)

			if test.output != nil {
				if test.output.Meta == nil {
					test.output.Meta = map[string]string{}
				}

				newPart := part{
					Content: string(resPart.Get()),
					Meta:    map[string]string{},
				}
				resPart.Metadata().Iter(func(k, v string) error {
					newPart.Meta[k] = v
					return nil
				})

				assert.Equal(t, *test.output, newPart)
			} else {
				assert.Nil(t, resPart)
			}
		})
	}
}

func TestTargets(t *testing.T) {
	function := func(name string, args ...interface{}) query.Function {
		t.Helper()
		fn, err := query.InitFunction(name, args...)
		require.NoError(t, err)
		return fn
	}

	metaKey := func(k string) *string {
		return &k
	}

	tests := []struct {
		mapping           *Executor
		queryTargets      []query.TargetPath
		assignmentTargets []TargetPath
	}{
		{
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("first")),
				NewStatement(nil, NewMetaAssignment(metaKey("bar")), query.NewLiteralFunction("second")),
				NewStatement(nil, NewVarAssignment("baz"), function("meta", "third")),
			),
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue, "first"),
				query.NewTargetPath(query.TargetMetadata, "third"),
			},
			assignmentTargets: []TargetPath{
				NewTargetPath(TargetValue, "foo"),
				NewTargetPath(TargetMetadata, "bar"),
				NewTargetPath(TargetVariable, "baz"),
			},
		},
		{
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewFieldFunction("first")),
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction("second")),
				NewStatement(nil, NewVarAssignment("baz"), function("meta", "third")),
			),
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue, "first"),
				query.NewTargetPath(query.TargetMetadata, "third"),
			},
			assignmentTargets: []TargetPath{
				NewTargetPath(TargetValue),
				NewTargetPath(TargetMetadata),
				NewTargetPath(TargetVariable, "baz"),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			assert.Equal(t, test.queryTargets, test.mapping.QueryTargets(query.TargetsContext{
				Maps: map[string]query.Function{},
			}))
			assert.Equal(t, test.assignmentTargets, test.mapping.AssignmentTargets())
		})
	}
}

func TestExec(t *testing.T) {
	metaKey := func(k string) *string {
		return &k
	}

	function := func(name string, args ...interface{}) query.Function {
		t.Helper()
		fn, err := query.InitFunction(name, args...)
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		mapping      *Executor
		input        interface{}
		output       interface{}
		outputString string
		err          string
	}{
		"cant set meta": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(metaKey("foo")), query.NewLiteralFunction("bar")),
			),
			err: "failed to assign mapping result at line 0: unable to assign metadata in the current context",
		},
		"cant use json": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), function("json", "bar")),
			),
			err: "failed to execute mapping assignment at line 0: target message part does not exist",
		},
		"simple root get and set": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewFieldFunction("")),
			),
			input:        "foobar",
			output:       "foobar",
			outputString: "foobar",
		},
		"nested get and set": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("bar")),
			),
			input:        map[string]interface{}{"bar": "baz"},
			output:       map[string]interface{}{"foo": "baz"},
			outputString: `{"foo":"baz"}`,
		},
		"failed get": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), function("json", "bar.baz")),
			),
			input:        map[string]interface{}{"nope": "baz"},
			err:          "failed to execute mapping assignment at line 0: target message part does not exist",
			outputString: "",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			res, err := test.mapping.Exec(query.FunctionContext{
				MsgBatch: message.New(nil),
			}.WithValue(test.input))
			if len(test.err) > 0 {
				require.EqualError(t, err, test.err)
			} else {
				assert.Equal(t, test.output, res)
			}
			resString := test.mapping.ToString(query.FunctionContext{
				MsgBatch: message.New(nil),
			}.WithValue(test.input))
			assert.Equal(t, test.outputString, resString)
			resBytes := test.mapping.ToBytes(query.FunctionContext{
				MsgBatch: message.New(nil),
			}.WithValue(test.input))
			assert.Equal(t, test.outputString, string(resBytes))
		})
	}
}

func TestQueries(t *testing.T) {
	type part struct {
		Content string
		Meta    map[string]string
	}

	initFunc := func(name string, args ...interface{}) query.Function {
		t.Helper()
		fn, err := query.InitFunction(name, args...)
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping *Executor
		output  bool
		err     error
	}{
		"simple json query": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewFieldFunction("bar")),
			),
			input:  []part{{Content: `{"bar":true}`}},
			output: true,
		},
		"simple json query 2": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewFieldFunction("bar")),
			),
			input:  []part{{Content: `{"bar":false}`}},
			output: false,
		},
		"simple json query bad type": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewFieldFunction("bar")),
			),
			input: []part{{Content: `{"bar":{"is":"an object"}}`}},
			err:   errors.New("expected bool value, found object"),
		},
		"var assignment": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewVarAssignment("foo"), query.NewLiteralFunction(true)),
				NewStatement(nil, NewJSONAssignment(), initFunc("var", "foo")),
			),
			input:  []part{{Content: `not valid json`}},
			output: true,
		},
		"meta query error": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), initFunc("meta", "foo")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to execute mapping query at line 0: metadata value 'foo' not found"),
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.New(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				for k, v := range p.Meta {
					part.Metadata().Set(k, v)
				}
				msg.Append(part)
			}

			res, err := test.mapping.QueryPart(test.index, msg)
			if test.err != nil {
				assert.EqualError(t, err, test.err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			}
		})
	}
}
