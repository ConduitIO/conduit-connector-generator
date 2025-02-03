# Conduit Connector Generator

The generator connector is one of [Conduit](https://github.com/ConduitIO/conduit)
builtin plugins. It generates sample records using its source connector. It has
no destination and trying to use that will result in an error.

## Configuration

> [!IMPORTANT]
> Parameters starting with `collections.*` are used to configure the format and
> operations for a specific collection. The `*` in the parameter name should be
> replaced with the collection name.

Below is a list of all available configuration parameters:

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "generator"
        settings:
          # Comma separated list of record operations to generate. Allowed
          # values are "create", "update", "delete", "snapshot".
          # Type: string
          collections.*.operations: "create"
          # Comma separated list of record operations to generate. Allowed
          # values are "create", "update", "delete", "snapshot".
          # Type: string
          operations: "create"
          # The amount of time the generator is generating records in a burst.
          # Has an effect only if `burst.sleepTime` is set.
          # Type: duration
          burst.generateTime: "1s"
          # The time the generator "sleeps" between bursts.
          # Type: duration
          burst.sleepTime: "0s"
          # The options for the `raw` and `structured` format types. It accepts
          # pairs of field names and field types, where the type can be one of:
          # `int`, `string`, `time`, `bool`, `duration`.
          # Type: string
          collections.*.format.options.*: ""
          # Path to the input file (only applicable if the format type is
          # `file`).
          # Type: string
          collections.*.format.options.path: ""
          # The format of the generated payload data (raw, structured, file).
          # Type: string
          collections.*.format.type: ""
          # The options for the `raw` and `structured` format types. It accepts
          # pairs of field names and field types, where the type can be one of:
          # `int`, `string`, `time`, `bool`, `duration`.
          # Type: string
          format.options.*: ""
          # Path to the input file (only applicable if the format type is
          # `file`).
          # Type: string
          format.options.path: ""
          # The format of the generated payload data (raw, structured, file).
          # Type: string
          format.type: ""
          # The maximum rate in records per second, at which records are
          # generated (0 means no rate limit).
          # Type: float
          rate: "0.0"
          # The time it takes to 'read' a record. Deprecated: use `rate`
          # instead.
          # Type: duration
          readTime: "0s"
          # Number of records to be generated (0 means infinite).
          # Type: int
          recordCount: "0"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Examples

### Bursts

The following configuration generates 100 records in bursts of 10 records each,
with a 1-second sleep time between bursts.

> [!NOTE]
> The generator currently has no concept of resuming work. For instance, below
> we have configured it to generate 100 records, but if we restart the pipeline
> (by stopping and starting the pipeline or by restarting Conduit), then it will
> start generating the 100 records from scratch.

```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        type: source
        plugin: generator
        settings:
          # global settings
          rate: 10
          recordCount: 100
          burst.generateTime: 1s
          burst.sleepTime: 1s
          # default collection
          format.type: structured
          format.options.id: int
          format.options.name: string
          operations: create
```

### Collections

The following configuration generates records forever with a steady rate of 1000
records per second. Records are generated in the `users` and `orders` collections.
The generated records have a different format, depending on the collection they
belong to.

```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        type: source
        plugin: generator
        settings:
          # global settings
          rate: 1000
          # collection "users"
          collections.users.format.type: structured
          collections.users.format.options.id: int
          collections.users.format.options.name: string
          collections.users.operations: create
          # collection "orders"
          collections.orders.format.type: raw
          collections.orders.format.options.id: int
          collections.orders.format.options.product: string
          collections.orders.operations: create,update,delete
```

## How to build it

Run `make`.

## Testing

Run `make test` to run all the unit tests.
