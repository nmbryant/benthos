package config

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v3"
)

func TestComponentExamples(t *testing.T) {
	testComponent := func(componentType, typeName, title, conf string) {
		s := New()
		dec := yaml.NewDecoder(bytes.NewReader([]byte(conf)))
		dec.KnownFields(true)
		assert.NoError(t, dec.Decode(&s), "%v:%v:%v", componentType, typeName, title)

		lints, err := Lint([]byte(conf), s)
		assert.NoError(t, err, "%v:%v:%v", componentType, typeName, title)
		for _, lint := range lints {
			t.Errorf("%v %v:%v:%v", lint, componentType, typeName, title)
		}

		type confAlias Type
		sAliased := confAlias(New())
		dec = yaml.NewDecoder(bytes.NewReader([]byte(conf)))
		dec.KnownFields(true)
		assert.NoError(t, dec.Decode(&sAliased), "%v:%v:%v", componentType, typeName, title)
	}

	for typeName, ctor := range input.Constructors {
		for _, example := range ctor.Examples {
			testComponent("input", typeName, example.Title, example.Config)
		}
	}
	for typeName, ctor := range processor.Constructors {
		for _, example := range ctor.Examples {
			testComponent("processor", typeName, example.Title, example.Config)
		}
	}
	for typeName, ctor := range output.Constructors {
		for _, example := range ctor.Examples {
			testComponent("output", typeName, example.Title, example.Config)
		}
	}
}

func CheckTagsOfType(v reflect.Type, checkedTypes map[string]struct{}, t *testing.T) {
	tPath := v.PkgPath() + "." + v.Name()
	if _, exists := checkedTypes[tPath]; len(v.PkgPath()) > 0 && exists {
		return
	}
	checkedTypes[tPath] = struct{}{}

	switch v.Kind() {
	case reflect.Slice:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Map:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			jTag := field.Tag.Get("json")
			yTag := field.Tag.Get("yaml")

			if len(field.PkgPath) > 0 {
				continue
			}

			if len(yTag) == 0 {
				t.Errorf("Empty field '%v' tag in type %v", field.Name, tPath)
			}

			if strings.ToLower(yTag) != yTag {
				t.Errorf("Non-lower case field '%v' tag in type %v: %v", field.Name, tPath, yTag)
			}

			if jTag != yTag {
				t.Errorf("Mismatched field '%v' config tags in type %v: json(%v) != yaml(%v)", field.Name, tPath, jTag, yTag)
			}

			CheckTagsOfType(field.Type, checkedTypes, t)
		}
	}
}

func TestConfigTags(t *testing.T) {
	v := reflect.TypeOf(New())

	checkedTypes := map[string]struct{}{}
	CheckTagsOfType(v, checkedTypes, t)
}

func TestExampleGen(t *testing.T) {
	conf := New()
	AddExamples(&conf, "files", "memory", "jmespath", "file")

	jBytes, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	gObj, err := gabs.ParseJSON(jBytes)
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := `"files"`, gObj.Path("input.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"memory"`, gObj.Path("buffer.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["jmespath","filter_parts"]`, gObj.Path("pipeline.processors.*.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["text","jmespath"]`, gObj.Path("pipeline.processors.*.filter_parts.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"file"`, gObj.Path("output.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}
}
