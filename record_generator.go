// Copyright © 2022 Meroxa, Inc.
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

package generator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

type recordGenerator struct {
	config RecordConfig
	cached sdk.RawData
}

func newRecordGenerator(config RecordConfig) *recordGenerator {
	return &recordGenerator{config: config}
}

func (g *recordGenerator) init() error {
	if g.config.FormatType != FormatFile {
		return nil
	}
	// files are cached, so that the time to read files
	// (which grows as the file size grows)
	// doesn't affect generator read times and the message rate
	// any implications on Conduit's resource usage
	// need to be taken into account while testing
	bytes, err := os.ReadFile(g.config.FormatOptions.(string))
	if err != nil {
		return fmt.Errorf("failed reading file: %w", err)
	}
	g.cached = bytes
	return nil
}

func (g *recordGenerator) generate() (sdk.Record, error) {
	p, err := g.generatePayload(g.config)
	if err != nil {
		return sdk.Record{}, err
	}
	operation, err := g.generateOperation()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error generating record's operation")
	}
	var metadata sdk.Metadata
	metadata = make(map[string]string)
	metadata.SetReadAt(time.Now())
	return sdk.Record{
		Position:  []byte(uuid.New().String()),
		Operation: operation,
		Metadata:  metadata,
		Key:       sdk.RawData(uuid.NewString()),
		Payload: sdk.Change{
			After: p,
		}}, nil
}

func (g *recordGenerator) generatePayload(config RecordConfig) (sdk.Data, error) {
	switch config.FormatType {
	case FormatFile:
		return g.generateFilePayload()
	case FormatRaw, FormatStructured:
		return g.generateStruct(config.FormatType, config.FormatOptions.(map[string]string))
	default:
		return nil, fmt.Errorf("unrecognized type of payload to generate: %q", config.FormatType)
	}
}

func (g *recordGenerator) generateOperation() (sdk.Operation, error) {
	switch g.config.Operation {
	case "create":
		return sdk.OperationCreate, nil
	case "update":
		return sdk.OperationUpdate, nil
	case "snapshot":
		return sdk.OperationSnapshot, nil
	case "delete":
		return sdk.OperationDelete, nil
	case "random":
		// generate a random int from 1 to 4
		randNum := rand.Int63n(4) + 1
		return sdk.Operation(randNum), nil
	default:
		return sdk.OperationCreate, fmt.Errorf("unrecognized type of payload to generate: %q", g.config.FormatType)
	}
}

func (g *recordGenerator) generateFilePayload() (sdk.Data, error) {
	return g.cached, nil
}

func (g *recordGenerator) generateStruct(format string, fields map[string]string) (sdk.Data, error) {
	return g.toData(format, g.newRecord(fields))
}

func (g *recordGenerator) newRecord(fields map[string]string) map[string]interface{} {
	rec := make(map[string]interface{})
	for name, typeString := range fields {
		rec[name] = g.newDummyValue(typeString)
	}
	return rec
}

func (g *recordGenerator) newDummyValue(typeString string) interface{} {
	switch typeString {
	case "int":
		return rand.Int31() //nolint:gosec // security not important here
	case "string":
		return "string " + uuid.NewString()
	case "time":
		return time.Now()
	case "bool":
		return rand.Int()%2 == 0 //nolint:gosec // security not important here
	default:
		panic(errors.New("invalid field"))
	}
}

func (g *recordGenerator) toData(format string, rec map[string]interface{}) (sdk.Data, error) {
	switch format {
	case FormatRaw:
		return g.toRawData(rec)
	case FormatStructured:
		return sdk.StructuredData(rec), nil
	default:
		return nil, fmt.Errorf("unknown format request %q", format)
	}
}

func (g *recordGenerator) toRawData(rec map[string]interface{}) (sdk.RawData, error) {
	bytes, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("couldn't serialize data: %w", err)
	}
	return bytes, nil
}
