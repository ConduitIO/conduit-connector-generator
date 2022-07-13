# Conduit Connector Generator

### General

The generator connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It generates sample
records using its source connector. It has no destination and trying to use that will result in an error.

### How to build it

Run `make`.

### Testing

Run `make test` to run all the unit tests.

### How it works

The data is generated in JSON format. The JSON objects themselves are generated using a field specification, which is
explained in more details in the [Configuration section](#Configuration) below.

The connector is great for getting started with Conduit but also for certain types of performance tests.

### Configuration

| name        | description                                                                                                                                        | required | Default | Example                                                                                                 |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|---------------------------------------------------------------------------------------------------------|
| recordCount | Number of records to be generated. -1 for no limit.                                                                                                | false    | "-1"    | 15 (generates 15 records)                                                                               |
| readTime    | The time it takes to 'read' a record.                                                                                                              | false    | "0s"    | 100ms (generates records every 100ms)                                                                   |
| fields      | A comma-separated list of name:type tokens, <br/>where type can be: int, string, time, bool.                                                       | true     | ""      | id:int,name:string <br/> (generates a struct with an ID field, type int, and a name field, type string) |
| format      | Format of the generated payload data: raw, structured.<br/> If fields are specified, then the payload will be a JSON string's byte representation. | false    | "raw"   |                                                                                                         |
| payloadFile | Path to file containing the payload for generated records.                                                                                         | false    | ""      |                                                                                                         |
