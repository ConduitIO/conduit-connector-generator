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

type Config struct {
	RecordCount int64
	ReadTime    time.Duration

	FormatType       string
	FormatOptions    string
	PayloadGenerator PayloadGenerator
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

	// check if it's a recognized format
	switch config[FormatType] {
	case FormatRaw, FormatStructured, FormatFile:
		break
	default:
		return Config{}, fmt.Errorf("unknown payload format %q", config[FormatType])
	}

	pg, err := NewPayloadGenerator(config[FormatType], config[FormatOptions])
	if err != nil {
		return Config{}, fmt.Errorf("failed configuring payload generator: %w", err)
	}
	parsed.PayloadGenerator = pg

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
