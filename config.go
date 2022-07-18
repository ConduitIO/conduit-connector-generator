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
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	RecordCount   = "recordCount"
	ReadTime      = "readTime"
	FormatType    = "format.type"
	FormatOptions = "format.options"

	FormatRaw        = "raw"
	FormatStructured = "structured"
	FormatFile       = "file"
)

var (
	knownFieldTypes = []string{"int", "string", "time", "bool"}
	requiredFields  = []string{FormatType, FormatOptions}
)

type RecordConfig struct {
	FormatType    string
	FormatOptions map[string]interface{}
}

func ParseRecordConfig(formatType, formatOptions string) (RecordConfig, error) {
	c := RecordConfig{
		FormatOptions: make(map[string]interface{}),
	}
	// check if it's a recognized format
	switch formatType {
	case FormatFile:
		c.FormatType = FormatFile
		c.FormatOptions["path"] = formatOptions
	case FormatStructured, FormatRaw:
		c.FormatType = formatType
		fields, err := parseFields(formatOptions)
		if err != nil {
			return RecordConfig{}, fmt.Errorf("failed parsing fields: %w", err)
		}
		c.FormatOptions["fields"] = fields
	default:
		return RecordConfig{}, fmt.Errorf("unknown payload format %q", formatType)
	}

	return c, nil
}

type Config struct {
	RecordCount int64
	ReadTime    time.Duration

	RecordConfig RecordConfig
}

func Parse(config map[string]string) (Config, error) {
	err := checkRequired(config)
	if err != nil {
		return Config{}, err
	}
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

	rc, err := ParseRecordConfig(config[FormatType], config[FormatOptions])
	if err != nil {
		return Config{}, fmt.Errorf("failed configuring payload generator: %w", err)
	}
	parsed.RecordConfig = rc

	return parsed, nil
}

func checkRequired(cfg map[string]string) error {
	var missing []string
	for _, reqKey := range requiredFields {
		_, exists := cfg[reqKey]
		if !exists {
			missing = append(missing, reqKey)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("required parameters missing %v", missing)
	}
	return nil
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
