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
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:              "generator",
		Summary:           "Generator plugin",
		Description:       "A plugin capable of generating dummy records (in JSON format).",
		Version:           "v0.1.0",
		Author:            "Meroxa, Inc.",
		DestinationParams: map[string]sdk.Parameter{},
		SourceParams: map[string]sdk.Parameter{
			RecordCount: {
				Default:     "-1",
				Required:    false,
				Description: "Number of records to be generated. -1 for no limit.",
			},
			ReadTime: {
				Default:     "0s",
				Required:    false,
				Description: "The time it takes to 'read' a record.",
			},
			SleepTime: {
				Default:     "0s",
				Required:    false,
				Description: "The time the generator 'sleeps' before it starts generating records. Must be non-negative.",
			},
			GenerateTime: {
				Default:     "max. duration in Go",
				Required:    false,
				Description: "The amount of time the generator is generating records. Must be positive.",
			},
			Fields: {
				Default:     "",
				Required:    true,
				Description: "A comma-separated list of name:type tokens, where type can be: int, string, time, bool.",
			},
			Format: {
				Default:     FormatRaw,
				Required:    false,
				Description: "Format of the generated payload data: raw, structured.",
			},
		},
	}
}
