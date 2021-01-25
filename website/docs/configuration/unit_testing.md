---
title: Unit Testing
---

The Benthos service offers a command `benthos test ./...` for running unit tests on sections of a configuration file. This makes it easy to protect your config files from regressions over time.

## Contents

1. [Writing a Test](#writing-a-test)
2. [Output Conditions](#output-conditions)
3. [Running Tests](#running-tests)

## Writing a Test

Let's imagine we have a configuration file `foo.yaml` containing some processors:

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - bloblang: '"%vend".format(content().uppercase().string())'

output:
  aws_s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

One way to write our unit tests for this config is to accompany it with a file of the same name and extension but suffixed with `_benthos_test`, which in this case would be `foo_benthos_test.yaml`. We can generate an example definition for this config with `benthos test --generate ./foo.yaml` which gives:

```yml
parallel: true
tests:
  - name: example test
    target_processors: '/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
        metadata:
          example_key: example metadata value
    output_batches:
      -
        - content_equals: example content
          metadata_equals:
            example_key: example metadata value
```

The field `parallel` instructs as to whether the tests listed in this definition should be executed in parallel. Under `tests` we then have a list of any number of unit tests to execute for the config file.

Each test is run in complete isolation, including any resources defined by the config file. Tests should be allocated a unique `name` that identifies the feature being tested.

The field `target_processors` is a [JSON Pointer][json-pointer] that identifies the specific processors within the file which should be executed by the test. This allows you to target a specific processor (`/pipeline/processors/0`), or processors within a different section on your config (`/input/broker/inputs/0/processors`) if required.

The field `environment` allows you to define an object of key/value pairs that set environment variables to be evaluated during the parsing of the target config file. These are unique to each test, allowing you to test different environment variable interpolation combinations.

> When tests are run in parallel they will NOT retain their environment variables during execution. In order to retain custom environment variables ensure that `parallel` is set to `false`.

The field `input_batch` lists one or more messages to be fed into the targeted processors as a batch. Each message of the batch may have its raw content defined as well as metadata key/value pairs.

For the common case where the messages are in JSON format, you can use `json_content` instead of `content` to specify the message structurally rather than verbatim.

The field `output_batches` lists any number of batches of messages which are expected to result from the target processors. Each batch lists any number of messages, each one defining [`conditions`](#output-conditions) to describe the expected contents of the message.

If the number of batches defined does not match the resulting number of batches the test will fail. If the number of messages defined in each batch does not match the number in the resulting batches the test will fail. If any condition of a message fails then the test fails.

### Inline Tests

Sometimes it's more convenient to define your tests within the config being tested. This is fine, simply add the `tests` field to the end of the config being tested.

### Fragmented Tests

Sometimes the number of tests you need to define in order to cover a config file is so vast that it's necessary to split them across multiple test definition files. This is possible but Benthos still requires a way to detect the configuration file being targeted by these fragmented test definition files, which we can do by prefixing our `target_processors` field with the path of the target relative to the definition file.

The syntax of `target_processors` in this case is a full [JSON Pointer][json-pointer] that should look something like `target.yaml#/pipeline/processors`. For example, if we saved our test definition above in an arbitrary location like `./tests/first.yaml` and wanted to target our original `foo.yaml` config file, we could do that with the following:

```yml
tests:
  - name: example test
    target_processors: '../foo.yaml#/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
        metadata:
          example_key: example metadata value
    output_batches:
      -
        - content_equals: example content
          metadata_equals:
            example_key: example metadata value
```

## Output Conditions

### `bloblang`

```yml
bloblang: 'this.age > 10 && meta("foo").length() > 0'
```

Executes a [Bloblang expression][bloblang] on a message, if the result is anything other than a boolean equalling `true` the test fails.

### `content_equals`

```yml
content_equals: example content
```

Checks the full raw contents of a message against a value.

### `content_matches`

```yml
content_matches: "^foo [a-z]+ bar$"
```

Checks whether the full raw contents of a message matches a regular expression (re2).

### `metadata_equals`

```yml
metadata_equals:
  example_key: example metadata value
```

Checks a map of metadata keys to values against the metadata stored in the message. If there is a value mismatch between a key of the condition versus the message metadata this condition will fail.

### `json_equals`

```yml
json_equals: { "key": "value" }
```

Checks that both the message and the condition are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences.

You can also structure the condition content as YAML and it will be converted to the equivalent JSON document for testing:

```yml
json_equals:
  key: value
```

### `json_contains`

```yml
json_contains: { "key": "value" }
```

Checks that both the message and the condition are valid JSON documents, and that the message is a superset of the condition.

## Running Tests

Executing tests for a specific config can be done by pointing the subcommand `test` at either the config to be tested or its test definition, e.g. `benthos test ./config.yaml` and `benthos test ./config_benthos_test.yaml` are equivalent.

In order to execute all tests of a directory simply point `test` to that directory, e.g. `benthos test ./foo` will execute all tests found in the directory `foo`. In order to walk a directory tree and execute all tests found you can use the shortcut `./...`, e.g. `benthos test ./...` will execute all tests found in the current directory, any child directories, and so on.

[json-pointer]: https://tools.ietf.org/html/rfc6901
[bloblang]: /docs/guides/bloblang/about
