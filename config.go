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
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	RecordCount  = "recordCount"
	ReadTime     = "readTime"
	SleepTime    = "sleepTime"
	GenerateTime = "generateTime"
	Fields       = "fields"
	Format       = "format"

	FormatRaw        = "raw"
	FormatStructured = "structured"
)

var knownFieldTypes = []string{"int", "string", "time", "bool"}

type Config struct {
	RecordCount  int64
	ReadTime     time.Duration
	SleepTime    time.Duration
	GenerateTime time.Duration
	Fields       map[string]string
	Format       string
}

func Parse(config map[string]string) (Config, error) {
	dh := durationHelper{}
	parsed := Config{}
	// default value
	parsed.RecordCount = -1
	if recCount, ok := config[RecordCount]; ok {
		recCountParsed, err := strconv.ParseInt(recCount, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid record count: %w", err)
		}
		parsed.RecordCount = recCountParsed
	}

	readTime, err := dh.parseNonNegative(config[ReadTime], time.Duration(0))
	if err != nil {
		return Config{}, fmt.Errorf("invalid read time: %w", err)
	}
	parsed.ReadTime = readTime

	sleepTime, err := dh.parseNonNegative(config[SleepTime], time.Duration(0))
	if err != nil {
		return Config{}, fmt.Errorf("invalid sleep time: %w", err)
	}
	parsed.SleepTime = sleepTime

	genTime, err := dh.parsePositive(config[GenerateTime], time.Duration(math.MaxInt64))
	if err != nil {
		return Config{}, fmt.Errorf("invalid generate time: %w", err)
	}
	parsed.GenerateTime = genTime

	parsed.Format = FormatRaw // default
	switch config[Format] {
	case FormatRaw, FormatStructured:
		parsed.Format = config[Format]
	case "":
		// leave default
	default:
		return Config{}, fmt.Errorf("unknown payload format %q", config[Format])
	}

	fieldsConcat := config[Fields]
	if fieldsConcat == "" {
		return Config{}, errors.New("no fields specified")
	}

	fieldsMap := map[string]string{}
	fields := strings.Split(fieldsConcat, ",")
	for _, field := range fields {
		if strings.Trim(field, " ") == "" {
			return Config{}, fmt.Errorf("got empty field spec in %q", field)
		}
		fieldSpec := strings.Split(field, ":")
		if validFieldSpec(fieldSpec) {
			return Config{}, fmt.Errorf("invalid field spec %q", field)
		}
		if !knownType(fieldSpec[1]) {
			return Config{}, fmt.Errorf("unknown data type in %q", field)
		}
		fieldsMap[fieldSpec[0]] = fieldSpec[1]
	}
	parsed.Fields = fieldsMap

	return parsed, nil
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
