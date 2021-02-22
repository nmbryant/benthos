---
title: mongodb
type: output
status: stable
categories: ["Services"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/output/mongodb.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Inserts items into a MongoDB collection.


<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

<TabItem value="common">

```yaml
# Common config fields, showing default values
output:
  mongodb:
    url: mongodb://localhost:27017
    database: ""
    collection: ""
    username: ""
    password: ""
    operation: update-one
    write_concern:
      w: ""
      j: false
      w_timeout: ""
    document_map: ""
    filter_map: ""
    hint_map: ""
    max_in_flight: 1
    batching:
      count: 0
      byte_size: 0
      period: ""
      check: ""
```

</TabItem>
<TabItem value="advanced">

```yaml
# All config fields, showing default values
output:
  mongodb:
    url: mongodb://localhost:27017
    database: ""
    collection: ""
    username: ""
    password: ""
    operation: update-one
    write_concern:
      w: ""
      j: false
      w_timeout: ""
    document_map: ""
    filter_map: ""
    hint_map: ""
    max_in_flight: 1
    batching:
      count: 0
      byte_size: 0
      period: ""
      check: ""
      processors: []
    max_retries: 3
    backoff:
      initial_interval: 1s
      max_interval: 5s
      max_elapsed_time: 30s
```

</TabItem>
</Tabs>


## Performance

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.

This output benefits from sending messages as a batch for improved performance.
Batches can be formed at both the input and output level. You can find out more
[in this doc](/docs/configuration/batching).

## Fields

### `url`

The URL of the target MongoDB DB.


Type: `string`  
Default: `"mongodb://localhost:27017"`  

```yaml
# Examples

url: mongodb://localhost:27017
```

### `database`

The name of the target MongoDB DB.


Type: `string`  
Default: `""`  

### `collection`

The name of the target collection in the MongoDB DB.


Type: `string`  
Default: `""`  

### `username`

The username to connect to the database.


Type: `string`  
Default: `""`  

### `password`

The password to connect to the database.


Type: `string`  
Default: `""`  

### `operation`

The mongo operation to perform. Must be one of the following: insert-one, delete-one, delete-many, replace-one, update-one.


Type: `string`  
Default: `"update-one"`  

### `write_concern`

The write concern settings for the mongo connection.


Type: `object`  

### `write_concern.w`

W requests acknowledgement that write operations propagate to the specified number of mongodb instances.


Type: `string`  
Default: `""`  

### `write_concern.j`

J requests acknowledgement from MongoDB that write operations are written to the journal.


Type: `bool`  
Default: `false`  

### `write_concern.w_timeout`

The write concern timeout.


Type: `string`  
Default: `""`  

### `document_map`

A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by mapping the fields in the message to the mongodb fields. The document map is required for the operations insert-one, replace-one and update-one.


Type: `array`  
Default: `""`  

```yaml
# Examples

document_map:
  - |-
    root.a = this.foo
    root.b = this.bar
```

### `filter_map`

A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should have the fields required to locate the document to delete.


Type: `array`  
Default: `""`  

```yaml
# Examples

filter_map:
  - |-
    root.a = this.foo
    root.b = this.bar
```

### `hint_map`

A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations except insert-one. It is used to improve performance of finding the documents in the mongodb.


Type: `array`  
Default: `""`  

```yaml
# Examples

hint_map:
  - |-
    root.a = this.foo
    root.b = this.bar
```

### `max_in_flight`

The maximum number of messages to have in flight at a given time. Increase this to improve throughput.


Type: `number`  
Default: `1`  

### `batching`

Allows you to configure a [batching policy](/docs/configuration/batching).


Type: `object`  

```yaml
# Examples

batching:
  byte_size: 5000
  count: 0
  period: 1s

batching:
  count: 10
  period: 1s

batching:
  check: this.contains("END BATCH")
  count: 0
  period: 1m
```

### `batching.count`

A number of messages at which the batch should be flushed. If `0` disables count based batching.


Type: `number`  
Default: `0`  

### `batching.byte_size`

An amount of bytes at which the batch should be flushed. If `0` disables size based batching.


Type: `number`  
Default: `0`  

### `batching.period`

A period in which an incomplete batch should be flushed regardless of its size.


Type: `string`  
Default: `""`  

```yaml
# Examples

period: 1s

period: 1m

period: 500ms
```

### `batching.check`

A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should end a batch.


Type: `string`  
Default: `""`  

```yaml
# Examples

check: this.type == "end_of_transaction"
```

### `batching.processors`

A list of [processors](/docs/components/processors/about) to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op.


Type: `array`  
Default: `[]`  

```yaml
# Examples

processors:
  - archive:
      format: lines

processors:
  - archive:
      format: json_array

processors:
  - merge_json: {}
```

### `max_retries`

The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.


Type: `number`  
Default: `3`  

### `backoff`

Control time intervals between retry attempts.


Type: `object`  

### `backoff.initial_interval`

The initial period to wait between retry attempts.


Type: `string`  
Default: `"1s"`  

### `backoff.max_interval`

The maximum period to wait between retry attempts.


Type: `string`  
Default: `"5s"`  

### `backoff.max_elapsed_time`

The maximum period to wait before retry attempts are abandoned. If zero then no limit is used.


Type: `string`  
Default: `"30s"`  

