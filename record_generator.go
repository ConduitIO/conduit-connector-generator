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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"io/ioutil"
	"math/rand"
	"time"
)

type RecordGenerator func() (sdk.Record, error)

func NewRecordGenerator(config RecordConfig) RecordGenerator {
	return func() (sdk.Record, error) {
		p, err := generatePayload(config)
		if err != nil {

		}
		return sdk.Record{
			Position:  []byte(uuid.New().String()),
			Metadata:  nil,
			Key:       sdk.RawData(uuid.NewString()),
			Payload:   p,
			CreatedAt: time.Now(),
		}, nil
	}
}

func generatePayload(config RecordConfig) (sdk.Data, error) {
	switch config.FormatType {
	case FormatFile:
		return generateFilePayload(config.FormatOptions["path"].(string))
	case FormatRaw, FormatStructured:
		return generateStruct(config.FormatType, config.FormatOptions["fields"].(map[string]string))
	default:
		return nil, fmt.Errorf("unrecognized type of payload to generate: %q", config.FormatType)
	}
}

func generateStruct(format string, fields map[string]string) (sdk.Data, error) {
	return toData(format, newRecord(fields))
}

func generateFilePayload(path string) (sdk.Data, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading file: %w", err)
	}

	return sdk.RawData(bytes), nil
}

func newRecord(fields map[string]string) map[string]interface{} {
	rec := make(map[string]interface{})
	for name, typeString := range fields {
		rec[name] = newDummyValue(typeString)
	}
	return rec
}

func newDummyValue(typeString string) interface{} {
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

func toData(format string, rec map[string]interface{}) (sdk.Data, error) {
	switch format {
	case FormatRaw:
		return toRawData(rec)
	case FormatStructured:
		return sdk.StructuredData(rec), nil
	default:
		return nil, fmt.Errorf("unknown format request %q", format)
	}
}

func toRawData(rec map[string]interface{}) (sdk.RawData, error) {
	bytes, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("couldn't serialize data: %w", err)
	}
	return bytes, nil
}
