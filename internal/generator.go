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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/goccy/go-json"
)

var KnownTypes = []string{"int", "string", "time", "bool"}

// RecordGenerator is an interface for generating records.
type RecordGenerator interface {
	// Next generates the next record.
	Next() sdk.Record
}

type baseRecordGenerator struct {
	collection   string
	operations   []sdk.Operation
	generateData func() sdk.Data

	count int
}

func (g *baseRecordGenerator) Next() sdk.Record {
	g.count++

	metadata := make(sdk.Metadata)
	metadata.SetReadAt(time.Now())
	if g.collection != "" {
		metadata.SetCollection(g.collection)
	}

	rec := sdk.Record{
		Position:  sdk.Position(strconv.Itoa(g.count)),
		Operation: g.operations[rand.Intn(len(g.operations))],
		Metadata:  metadata,
		Key:       sdk.RawData(randomWord()),
	}

	switch rec.Operation {
	case sdk.OperationSnapshot, sdk.OperationCreate:
		rec.Payload.After = g.generateData()
	case sdk.OperationUpdate:
		rec.Payload.Before = g.generateData()
		rec.Payload.After = g.generateData()
	case sdk.OperationDelete:
		rec.Payload.Before = g.generateData()
	}

	return rec
}

// NewFileRecordGenerator creates a RecordGenerator that reads the contents of a
// file at the given path. The file is read once and cached in memory. The
// RecordGenerator will generate records with the contents of the file as the
// payload data.
func NewFileRecordGenerator(
	collection string,
	operations []sdk.Operation,
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
		generateData: func() sdk.Data {
			return sdk.RawData(bytes)
		},
	}, nil
}

// NewStructuredRecordGenerator creates a RecordGenerator that generates records
// with structured data. The fields map should contain the field names and types
// for the structured data. The types can be one of: int, string, time, bool.
func NewStructuredRecordGenerator(
	collection string,
	operations []sdk.Operation,
	fields map[string]string,
) (RecordGenerator, error) {
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() sdk.Data {
			return randomStructuredData(fields)
		},
	}, nil
}

// NewRawRecordGenerator creates a RecordGenerator that generates records with
// raw data. The fields map should contain the field names and types for the raw
// data. The types can be one of: int, string, time, bool.
func NewRawRecordGenerator(
	collection string,
	operations []sdk.Operation,
	fields map[string]string,
) (RecordGenerator, error) {
	return &baseRecordGenerator{
		collection: collection,
		operations: operations,
		generateData: func() sdk.Data {
			return randomRawData(fields)
		},
	}, nil
}

func randomStructuredData(fields map[string]string) sdk.Data {
	data := make(sdk.StructuredData)
	for field, typ := range fields {
		switch typ {
		case "int":
			data[field] = rand.Int()
		case "string":
			data[field] = randomWord()
		case "time":
			data[field] = time.Now().UnixNano()
		case "bool":
			data[field] = rand.Int()%2 == 0
		default:
			panic(fmt.Errorf("field %q contains invalid type: %v", field, typ))
		}
	}
	return data
}

func randomRawData(fields map[string]string) sdk.RawData {
	data := randomStructuredData(fields)
	bytes, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Errorf("couldn't serialize data: %w", err))
	}
	return bytes
}
