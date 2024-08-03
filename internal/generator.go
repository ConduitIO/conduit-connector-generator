// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-commons/schema/avro"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
)

var KnownTypes = []string{"int", "string", "time", "bool", "duration"}

// RecordGenerator is an interface for generating records.
type RecordGenerator interface {
	// Next generates the next record.
	Next() opencdc.Record
}

type baseRecordGenerator struct {
	collection   string
	operations   []opencdc.Operation
	generateData func() opencdc.Data
	postProcess  func(opencdc.Record) opencdc.Record

	count int
}

func (g *baseRecordGenerator) Next() opencdc.Record {
	g.count++

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Now())
	if g.collection != "" {
		metadata.SetCollection(g.collection)
	}

	rec := opencdc.Record{
		Position:  opencdc.Position(strconv.Itoa(g.count)),
		Operation: g.operations[rand.Intn(len(g.operations))],
		Metadata:  metadata,
		Key:       opencdc.RawData(randomWord()),
	}

	switch rec.Operation {
	case opencdc.OperationSnapshot, opencdc.OperationCreate:
		rec.Payload.After = g.generateData()
	case opencdc.OperationUpdate:
		rec.Payload.Before = g.generateData()
		rec.Payload.After = g.generateData()
	case opencdc.OperationDelete:
		rec.Payload.Before = g.generateData()
	}

	if g.postProcess != nil {
		rec = g.postProcess(rec)
	}

	return rec
}

// NewFileRecordGenerator creates a RecordGenerator that reads the contents of a
// file at the given path. The file is read once and cached in memory. The
// RecordGenerator will generate records with the contents of the file as the
// payload data.
func NewFileRecordGenerator(
	collection string,
	operations []opencdc.Operation,
	path string,
) (RecordGenerator, error) {
	// Files are cached, so that the time to read files doesn't affect generator
	// read times and the message rate. This will increase Conduit's memory usage.
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return opencdc.RawData(bytes)
		},
	}, nil
}

// NewStructuredRecordGenerator creates a RecordGenerator that generates records
// with structured data. The fields map should contain the field names and types
// for the structured data. The types can be one of: int, string, time, bool.
func NewStructuredRecordGenerator(
	ctx context.Context,
	collection string,
	operations []opencdc.Operation,
	fields map[string]string,
	subject string,
) (RecordGenerator, error) {
	var postProcess func(opencdc.Record) opencdc.Record
	if subject != "" {
		if collection != "" {
			subject = collection + "." + subject
		}

		d := randomStructuredData(fields)

		srd, err := avro.SerdeForType(d)
		if err != nil {
			return nil, fmt.Errorf("failed to get serde for type: %w", err)
		}
		sch, err := sdkschema.Create(ctx, schema.TypeAvro, subject, []byte(srd.String()))
		if err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}

		postProcess = func(record opencdc.Record) opencdc.Record {
			schema.AttachPayloadSchemaToRecord(record, sch)
			return record
		}
	}

	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return randomStructuredData(fields)
		},
		postProcess: postProcess,
	}, nil
}

// NewRawRecordGenerator creates a RecordGenerator that generates records with
// raw data. The fields map should contain the field names and types for the raw
// data. The types can be one of: int, string, time, bool.
func NewRawRecordGenerator(
	collection string,
	operations []opencdc.Operation,
	fields map[string]string,
) (RecordGenerator, error) {
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() opencdc.Data {
			return randomRawData(fields)
		},
	}, nil
}

func randomStructuredData(fields map[string]string) opencdc.Data {
	data := make(opencdc.StructuredData)
	for field, typ := range fields {
		switch typ {
		case "int":
			data[field] = rand.Int()
		case "string":
			data[field] = randomWord()
		case "time":
			data[field] = time.Now().UTC()
		case "duration":
			data[field] = time.Duration(rand.Intn(1000)) * time.Second
		case "bool":
			data[field] = rand.Int()%2 == 0
		default:
			panic(fmt.Errorf("field %q contains invalid type: %v", field, typ))
		}
	}
	return data
}

func randomRawData(fields map[string]string) opencdc.RawData {
	data := randomStructuredData(fields)
	bytes, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Errorf("couldn't serialize data: %w", err))
	}
	return bytes
}
