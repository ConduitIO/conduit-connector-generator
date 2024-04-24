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
	"testing"

	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		have    Config
		wantErr string
	}{{
		name: "raw format",
		have: Config{
			CollectionConfig: CollectionConfig{
				Format: FormatConfig{
					Type: "raw",
					Options: map[string]string{
						"id": "int",
					},
				},
			},
		},
	}, {
		name: "structured format",
		have: Config{
			CollectionConfig: CollectionConfig{
				Format: FormatConfig{
					Type: "structured",
					Options: map[string]string{
						"id": "int",
					},
				},
			},
		},
	}, {
		name: "file format",
		have: Config{
			CollectionConfig: CollectionConfig{
				Format: FormatConfig{
					Type:            "file",
					FileOptionsPath: "/path/to/file.txt",
				},
			},
		},
	}, {
		name: "file format, no path",
		have: Config{
			CollectionConfig: CollectionConfig{
				Format: FormatConfig{
					Type: "file",
				},
			},
		},
		wantErr: "failed validating default collection: failed validating format: file path not specified",
	}, {
		name: "structured, invalid type",
		have: Config{
			CollectionConfig: CollectionConfig{
				Format: FormatConfig{
					Type: "structured",
					Options: map[string]string{
						"abc": "unknown",
					},
				},
			},
		},
		wantErr: `failed validating default collection: failed validating format: failed parsing fields: unknown data type in "abc"`,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := tc.have.Validate()
			if tc.wantErr != "" {
				is.True(err != nil)
				is.Equal(tc.wantErr, err.Error())
			} else {
				is.NoErr(err)
			}
		})
	}
}
