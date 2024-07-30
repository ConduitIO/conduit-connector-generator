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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-generator/internal"
	"golang.org/x/time/rate"
)

const (
	FormatTypeRaw        = "raw"
	FormatTypeStructured = "structured"
	FormatTypeFile       = "file"
)

type Config struct {
	Burst BurstConfig `json:"burst"`
	// Number of records to be generated (0 means infinite).
	RecordCount int `json:"recordCount" validate:"gt=-1"`
	// The time it takes to 'read' a record.
	// Deprecated: use `rate` instead.
	ReadTime time.Duration `json:"readTime"`
	// The maximum rate in records per second, at which records are generated (0
	// means no rate limit).
	Rate float64 `json:"rate"`

	// Configuration for default collection (i.e. records without a collection).
	// Kept for backwards compatibility.
	CollectionConfig
	Collections map[string]CollectionConfig `json:"collections"`
}

type BurstConfig struct {
	// The time the generator "sleeps" between bursts.
	SleepTime time.Duration `json:"sleepTime"`
	// The amount of time the generator is generating records in a burst. Has an
	// effect only if `burst.sleepTime` is set.
	GenerateTime time.Duration `json:"generateTime" default:"1s"`
}

type CollectionConfig struct {
	// Comma separated list of record operations to generate. Allowed values are
	// "create", "update", "delete", "snapshot".
	Operations []string     `json:"operations" default:"create" validate:"required"`
	Format     FormatConfig `json:"format"`
}

type FormatConfig struct {
	// The format of the generated payload data (raw, structured, file).
	Type string `json:"type" validate:"inclusion=raw|structured|file"`
	// The options for the `raw` and `structured` format types. It accepts pairs
	// of field names and field types, where the type can be one of: `int`, `string`, `time`, `bool`, `duration`.
	Options map[string]string `json:"options"`
	// Path to the input file (only applicable if the format type is `file`).
	FileOptionsPath string `json:"options.path"`
}

func (c Config) Validate() error {
	var errs []error

	// Validate readTime and rate.
	if c.ReadTime > 0 && c.Rate > 0 {
		errs = append(errs, errors.New(`cannot specify both "readTime" and "rate", "readTime" is deprecated, please only specify "rate"`))
	}
	if c.ReadTime < 0 {
		errs = append(errs, errors.New(`"readTime" should be greater or equal to 0`))
	}
	if c.Rate < 0 {
		errs = append(errs, errors.New(`"rate" should be greater or equal to 0`))
	}

	// Validate burst.
	if c.Burst.SleepTime < 0 {
		errs = append(errs, errors.New(`"burst.sleepTime" should be greater or equal to 0`))
	}
	if c.Burst.SleepTime > 0 && c.Burst.GenerateTime <= 0 {
		errs = append(errs, errors.New(`"burst.generateTime" should be greater than 0`))
	}

	// Validate collections.
	collections := c.GetCollectionConfigs()
	if len(collections) == 0 {
		errs = append(errs, errors.New("invalid configuration, please configure at least one collection using `format.type` or `collections.*.format.type`"))
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

func (c Config) RateLimit() rate.Limit {
	if c.Rate == 0 && c.ReadTime > 0 {
		// Convert read time to rate limit.
		return rate.Every(c.ReadTime)
	}
	return rate.Limit(c.Rate)
}

func (c Config) GetCollectionConfigs() map[string]CollectionConfig {
	collections := make(map[string]CollectionConfig, len(c.Collections)+1)
	if c.Format.Type != "" {
		collections[""] = c.CollectionConfig
	}
	for k, v := range c.Collections {
		collections[k] = v
	}
	return collections
}

func (c CollectionConfig) Validate() error {
	var errs []error

	_, err := c.parseOperations()
	if err != nil {
		errs = append(errs, err)
	}
	err = c.Format.Validate()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed validating format: %w", err))
	}

	return errors.Join(errs...)
}

func (c CollectionConfig) SdkOperations() []opencdc.Operation {
	// We can safely ignore the error here, it has been validated.
	op, _ := c.parseOperations()
	return op
}

func (c CollectionConfig) parseOperations() ([]opencdc.Operation, error) {
	operations := make([]opencdc.Operation, len(c.Operations))
	for i, raw := range c.Operations {
		var op opencdc.Operation
		err := op.UnmarshalText([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("failed parsing operation: %w", err)
		}
		operations[i] = op
	}
	return operations, nil
}

func (c FormatConfig) Validate() error {
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

func (c FormatConfig) validateFields(fields map[string]string) error {
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

func (c FormatConfig) knownType(typeString string) bool {
	for _, t := range internal.KnownTypes {
		if strings.ToLower(typeString) == t {
			return true
		}
	}
	return false
}
