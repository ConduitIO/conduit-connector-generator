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

//go:generate paramgen -output config_paramgen.go Config

package generator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	FormatTypeRaw        = "raw"
	FormatTypeStructured = "structured"
	FormatTypeFile       = "file"
)

type Config struct {
	Burst ConfigBurst `json:"burst"`
	// Number of records to be generated. -1 for no limit.
	RecordCount int `json:"recordCount" default:"0"`
	// The time it takes to 'read' a record.
	ReadTime time.Duration `json:"readTime" default:"0s"`

	// Configuration for default collection (i.e. records without a collection).
	// Kept for backwards compatibility.
	ConfigCollection
	Collections map[string]ConfigCollection `json:"collections"`
}

type ConfigBurst struct {
	// The time the generator 'sleeps' before it starts generating records. Must
	// be non-negative.
	SleepTime time.Duration `json:"sleepTime" default:"0s"`
	// The amount of time the generator is generating records. Must be positive.
	// If this option is empty, generator will generate records forever.
	GenerateTime time.Duration `json:"generateTime" default:"1s"`
}

type ConfigCollection struct {
	// The generated record operation.
	Operation string       `json:"operation" default:"created" validate:"required,inclusion=created|updated|deleted"`
	Format    ConfigFormat `json:"format"`
}

type ConfigFormat struct {
	// The format of the generated payload data (raw, structured, file).
	Type string `json:"type" validate:"inclusion=raw|structured|file"`
	// The options for the format type selected, which are:
	//   1. For raw and structured: pairs of field names and field types, where the type can be one of: int, string, time, bool.
	//   2. For the file format: a path to the file.
	Options map[string]string `json:"options"`
	// Path to the input file (only applicable if the format type is file).
	FileOptionsPath string `json:"options.path"`
}

func (c Config) Validate() error {
	var errs []error

	collections := c.GetConfigCollections()
	if len(collections) == 0 {
		return errors.New("invalid configuration, please configure at least one collection using `format.type` or `collections.*.format.type`")
	}
	for collection, cfg := range collections {
		err := cfg.Validate()
		if err != nil {
			if collection == "" {
				err = fmt.Errorf("failed validating default collection: %w", err)
			} else {
				err = fmt.Errorf("failed validating collection %q: %w", collection, err)
			}
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (c Config) GetConfigCollections() map[string]ConfigCollection {
	collections := make(map[string]ConfigCollection, len(c.Collections)+1)
	if c.Format.Type != "" {
		collections[""] = c.ConfigCollection
	}
	for k, v := range c.Collections {
		collections[k] = v
	}
	return collections
}

func (c ConfigCollection) Validate() error {
	var errs []error

	_, err := c.parseOperation()
	if err != nil {
		errs = append(errs, err)
	}
	err = c.Format.Validate()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed validating format: %w", err))
	}

	return errors.Join(errs...)
}

func (c ConfigCollection) SdkOperation() sdk.Operation {
	// We can safely ignore the error here, it has been validated.
	op, _ := c.parseOperation()
	return op
}

func (c ConfigCollection) parseOperation() (sdk.Operation, error) {
	var op sdk.Operation
	err := op.UnmarshalText([]byte(c.Operation))
	if err != nil {
		return 0, fmt.Errorf("failed parsing operation: %w", err)
	}
	return op, nil
}

func (c ConfigFormat) Validate() error {
	switch c.Type {
	case FormatTypeFile:
		if c.FileOptionsPath == "" {
			return errors.New("file path not specified")
		}
	case FormatTypeStructured, FormatTypeRaw:
		err := c.validateFields(c.Options)
		if err != nil {
			return fmt.Errorf("failed parsing fields: %w", err)
		}
	default:
		return fmt.Errorf("unknown format type %q", c.Type)
	}
	return nil
}

func (c ConfigFormat) validateFields(fields map[string]string) error {
	var errs []error
	for f, t := range fields {
		if strings.Trim(f, " ") == "" {
			errs = append(errs, fmt.Errorf("got empty field name in %q", f))
		}
		if strings.Trim(t, " ") == "" {
			errs = append(errs, fmt.Errorf("got empty type in %q", f))
		}
		if !c.knownType(t) {
			errs = append(errs, fmt.Errorf("unknown data type in %q", f))
		}
	}
	return errors.Join(errs...)
}

func (c ConfigFormat) knownType(typeString string) bool {
	knownFieldTypes := []string{"int", "string", "time", "bool"}
	for _, t := range knownFieldTypes {
		if strings.ToLower(typeString) == t {
			return true
		}
	}
	return false
}
