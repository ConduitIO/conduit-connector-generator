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
	"io/ioutil"
	"math/rand"
	"strings"
	"time"
)

type PayloadGenerator interface {
	Generate() (sdk.Data, error)
}

type filePayloadGenerator struct {
	path      string
	fileBytes []byte
}

func (f *filePayloadGenerator) Generate() (sdk.Data, error) {
	if f.fileBytes == nil {
		bytes, err := ioutil.ReadFile(f.path)
		if err != nil {
			return nil, fmt.Errorf("failed reading file: %w", err)
		}
		f.fileBytes = bytes
	}
	return sdk.RawData(f.fileBytes), nil
}

type structuredGenerator struct {
	fields  map[string]string
	format  string
	counter int64
}

func (s *structuredGenerator) Generate() (sdk.Data, error) {
	return s.toData(s.newRecord())
}

func (s *structuredGenerator) newRecord() map[string]interface{} {
	s.counter++
	rec := make(map[string]interface{})
	for name, typeString := range s.fields {
		rec[name] = s.newDummyValue(typeString, s.counter)
	}
	return rec
}

func (s *structuredGenerator) newDummyValue(typeString string, i int64) interface{} {
	switch typeString {
	case "int":
		return rand.Int31() //nolint:gosec // security not important here
	case "string":
		return fmt.Sprintf("string %v", i)
	case "time":
		return time.Now()
	case "bool":
		return rand.Int()%2 == 0 //nolint:gosec // security not important here
	default:
		panic(errors.New("invalid field"))
	}
}

func (s *structuredGenerator) toData(rec map[string]interface{}) (sdk.Data, error) {
	switch s.format {
	case FormatRaw:
		return s.toRawData(rec)
	case FormatStructured:
		return sdk.StructuredData(rec), nil
	default:
		return nil, fmt.Errorf("unknown format request %q", s.format)
	}
}

func (s *structuredGenerator) toRawData(rec map[string]interface{}) (sdk.RawData, error) {
	bytes, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("couldn't serialize data: %w", err)
	}
	return bytes, nil
}

func NewPayloadGenerator(formatType string, formatOptions string) (PayloadGenerator, error) {
	switch formatType {
	case FormatFile:
		return &filePayloadGenerator{path: formatOptions}, nil
	case FormatRaw, FormatStructured:
		fields, err := parseFields(formatOptions)
		if err != nil {
			return nil, fmt.Errorf("failed parsing field spec: %w", err)
		}
		return &structuredGenerator{
			fields: fields,
			format: formatType,
		}, nil
	default:
		return nil, fmt.Errorf("unknown format type %v", formatType)
	}
}

func parseFields(fieldsConcat string) (map[string]string, error) {
	if fieldsConcat == "" {
		return nil, nil
	}
	fieldsMap := map[string]string{}
	fields := strings.Split(fieldsConcat, ",")
	for _, field := range fields {
		if strings.Trim(field, " ") == "" {
			return nil, fmt.Errorf("got empty field spec in %q", field)
		}
		fieldSpec := strings.Split(field, ":")
		if validFieldSpec(fieldSpec) {
			return nil, fmt.Errorf("invalid field spec %q", field)
		}
		if !knownType(fieldSpec[1]) {
			return nil, fmt.Errorf("unknown data type in %q", field)
		}
		fieldsMap[fieldSpec[0]] = fieldSpec[1]
	}
	return fieldsMap, nil
}

func validFieldSpec(fieldSpec []string) bool {
	return len(fieldSpec) != 2 ||
		strings.Trim(fieldSpec[0], " ") == "" ||
		strings.Trim(fieldSpec[1], " ") == ""
}

func knownType(typeString string) bool {
	for _, t := range knownFieldTypes {
		if strings.ToLower(typeString) == t {
			return true
		}
	}
	return false
}
