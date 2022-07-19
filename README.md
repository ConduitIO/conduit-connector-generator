# Conduit Connector Generator

### General

The generator connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It generates sample
records using its source connector. It has no destination and trying to use that will result in an error.

Note that the generator currently has no concept of resuming work. For example, if you have configured it to generate 
100 records, let it run for some time, and then restart it (by restarting the pipeline or Conduit), then it will start 
generating the 100 records from scratch.

### How to build it

Run `make`.

### Testing

Run `make test` to run all the unit tests.

### How it works

The data is generated in JSON format. The JSON objects themselves are generated using a field specification, which is
explained in more details in the [Configuration section](#Configuration) below.

The connector is great for getting started with Conduit but also for certain types of performance tests.

It's possible to simulate a 'read' time for records. It's also possible to simulate bursts through "sleep and generate"
cycles, where the connector is sleeping for some time (not generating any records), then generating records for the 
specified time, and then repeating the same cycle. The connector always start with the sleeping phase.

### Configuration

#### recordCount
Number of records to be generated. -1 for no limit.
* Required: false
* Possible values: -1 or a non-negative number.
* Default: "-1"
* Example: "15" (generates 15 records)

#### readTime
The time it takes to 'read' a record.
* Required: false
* Possible values: A duration string, must not be negative. Also see: https://pkg.go.dev/time#ParseDuration
* Default: "0s"
* Example: "100ms" (generates records every 100ms)

#### format.type
Format of the generated payload data.
* Required: true
* Possible values: `raw`, `structured`, `file`
* Default: ""
* Example: "raw" (generates a record, with raw data payload)

#### format.options
An options string for the type of format specified in `format.type`.
* Required: true
* Possible values:
  * If `format.type: raw` or `format.type: structured`, `format.options` is a comma-separated list of name:type tokens,
    where type can be: int, string, time, bool. `format.type` will define how the payload will be serialized (it will be either
    raw or structured).
  * If `format.type: file`, `format.options` is a path to a file, which will be taken as a payload for the generated records.
* Default: ""
* Example: "id:int,name:string" (generates a struct with an ID field, type int, and a name field, type string)

#### burst.sleepTime
The time the generator 'sleeps' between bursts.
* Required: false
* Possible values: A duration string, must not be negative. Also see: https://pkg.go.dev/time#ParseDuration
* Default: "0s"
* Example: "30s" (the generator sleeps for 30 seconds, then started generating records)

#### burst.generateTime
The amount of time the generator is generating records. Has effect only if `burst.sleepTime` is set.
* Required: false
* Possible values: A duration string, must be positive. Also see: https://pkg.go.dev/time#ParseDuration
* Default: max. duration in Go
* Example: "60s" (the generator will be generating records for 60 seconds)
