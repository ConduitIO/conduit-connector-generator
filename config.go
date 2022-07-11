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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	RecordCount = "recordCount"
	ReadTime    = "readTime"
	Fields      = "fields"
	Format      = "format"
	PayloadFile = "payloadFile"

	FormatRaw        = "raw"
	FormatStructured = "structured"
)

var knownFieldTypes = []string{"int", "string", "time", "bool"}

type Config struct {
	RecordCount int64
	ReadTime    time.Duration
	Fields      map[string]string
	Format      string
	PayloadFile string
}

func Parse(config map[string]string) (Config, error) {
	parsed := Config{}

	// parse record count
	// default value
	parsed.RecordCount = -1
	if recCount, ok := config[RecordCount]; ok {
		recCountParsed, err := strconv.ParseInt(recCount, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid record count: %w", err)
		}
		parsed.RecordCount = recCountParsed
	}

	// parse read time
	if readTime, ok := config[ReadTime]; ok {
		readTimeParsed, err := time.ParseDuration(readTime)
		if err != nil || readTimeParsed < 0 {
			return Config{}, fmt.Errorf("invalid processing time: %w", err)
		}
		parsed.ReadTime = readTimeParsed
	}

	// parse payload file
	parsed.PayloadFile = config[PayloadFile]

	// parse payload format
	parsed.Format = FormatRaw // default
	switch config[Format] {
	case FormatRaw, FormatStructured:
		parsed.Format = config[Format]
	case "":
		// leave default
	default:
		return Config{}, fmt.Errorf("unknown payload format %q", config[Format])
	}

	// check if the PayloadFile parameter is compatible with other parameters
	if parsed.PayloadFile != "" && parsed.Format == FormatStructured {
		return Config{}, errors.New("payload file can only go with raw format")
	}

	// parse fields
	fieldsMap, err := parseFieldsMap(config)
	if err != nil {
		return Config{}, fmt.Errorf("failed parsing field spec: %w", err)
	}

	if len(fieldsMap) != 0 && parsed.PayloadFile != "" {
		return Config{}, errors.New("cannot specify fields and payload field at the same time")
	}
	if len(fieldsMap) == 0 && parsed.PayloadFile == "" {
		return Config{}, errors.New("no fields specified")
	}
	parsed.Fields = fieldsMap

	return parsed, nil
}

func parseFieldsMap(config map[string]string) (map[string]string, error) {
	fieldsConcat := config[Fields]
	fieldsMap := map[string]string{}
	if fieldsConcat == "" {
		return fieldsMap, nil
	}
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
	return len(fieldSpec) != 2 || strings.Trim(fieldSpec[0], " ") == "" || strings.Trim(fieldSpec[1], " ") == ""
}

func knownType(typeString string) bool {
	for _, t := range knownFieldTypes {
		if strings.ToLower(typeString) == t {
			return true
		}
	}
	return false
}
