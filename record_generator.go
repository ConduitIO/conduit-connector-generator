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
}

func (g recordGenerator) generate() (sdk.Record, error) {
	p, err := g.generatePayload(g.config)
	if err != nil {
		return sdk.Record{}, err
	}
	return sdk.Record{
		Position:  []byte(uuid.New().String()),
		Metadata:  make(map[string]string),
		Key:       sdk.RawData(uuid.NewString()),
		Payload:   p,
		CreatedAt: time.Now(),
	}, nil
}

func newRecordGenerator(config RecordConfig) recordGenerator {
	return recordGenerator{config: config}
}

func (g recordGenerator) generatePayload(config RecordConfig) (sdk.Data, error) {
	switch config.FormatType {
	case FormatFile:
		return g.generateFilePayload(config.FormatOptions["path"].(string))
	case FormatRaw, FormatStructured:
		return g.generateStruct(config.FormatType, config.FormatOptions["fields"].(map[string]string))
	default:
		return nil, fmt.Errorf("unrecognized type of payload to generate: %q", config.FormatType)
	}
}

func (g recordGenerator) generateFilePayload(path string) (sdk.Data, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading file: %w", err)
	}

	return sdk.RawData(bytes), nil
}

func (g recordGenerator) generateStruct(format string, fields map[string]string) (sdk.Data, error) {
	return g.toData(format, g.newRecord(fields))
}

func (g recordGenerator) newRecord(fields map[string]string) map[string]interface{} {
	rec := make(map[string]interface{})
	for name, typeString := range fields {
		rec[name] = g.newDummyValue(typeString)
	}
	return rec
}

func (g recordGenerator) newDummyValue(typeString string) interface{} {
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

func (g recordGenerator) toData(format string, rec map[string]interface{}) (sdk.Data, error) {
	switch format {
	case FormatRaw:
		return g.toRawData(rec)
	case FormatStructured:
		return sdk.StructuredData(rec), nil
	default:
		return nil, fmt.Errorf("unknown format request %q", format)
	}
}

func (g recordGenerator) toRawData(rec map[string]interface{}) (sdk.RawData, error) {
	bytes, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("couldn't serialize data: %w", err)
	}
	return bytes, nil
}
