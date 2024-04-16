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
			Format: ConfigFormat{
				Type:    "raw",
				Options: "id:int",
			},
		},
	}, {
		name: "structured format",
		have: Config{
			Format: ConfigFormat{
				Type:    "structured",
				Options: "id:int",
			},
		},
	}, {
		name: "file format",
		have: Config{
			Format: ConfigFormat{
				Type:    "file",
				Options: "/path/to/file.txt",
			},
		},
	}, {
		name: "file format, no path",
		have: Config{
			Format: ConfigFormat{
				Type:    "file",
				Options: "",
			},
		},
		wantErr: "file path not specified",
	}, {
		name: "structured, malformed fields, no type",
		have: Config{
			Format: ConfigFormat{
				Type:    "structured",
				Options: "abc:",
			},
		},
		wantErr: `failed parsing fields: invalid field spec "abc:"`,
	}, {
		name: "structured, malformed fields, name only",
		have: Config{
			Format: ConfigFormat{
				Type:    "structured",
				Options: "abc",
			},
		},
		wantErr: `failed parsing fields: invalid field spec "abc"`,
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

// func TestParse_Durations(t *testing.T) {
// 	testCases := []fieldTest[time.Duration]{
// 		{
// 			name: "default read time is 0s",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 			},
// 			expErr: "",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
// 		},
// 		{
// 			name: "negative read time not allowed",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 				ReadTime:      "-1s",
// 			},
// 			expErr: "invalid read time: duration cannot be negative",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
// 		},
// 		{
// 			name: "default sleep time is 0s",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 			},
// 			expErr: "",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
// 		},
// 		{
// 			name: "negative sleep time not allowed",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 				SleepTime:     "-1s",
// 			},
// 			expErr: "invalid sleep time: duration cannot be negative",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
// 		},
// 		{
// 			name: "negative generate time not allowed",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 				GenerateTime:  "-1s",
// 			},
// 			expErr: "invalid generate time: duration must be positive",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.GenerateTime },
// 		},
// 		{
// 			name: "generate time 0 not allowed",
// 			input: map[string]string{
// 				FormatType:    FormatRaw,
// 				FormatOptions: "id:int",
// 				GenerateTime:  "0ms",
// 			},
// 			expErr: "invalid generate time: duration must be positive",
// 			expVal: time.Duration(0),
// 			getter: func(cfg Config) time.Duration { return cfg.GenerateTime },
// 		},
// 	}
//
// 	for _, tc := range testCases {
// 		tc := tc
// 		t.Run(tc.name, func(t *testing.T) {
// 			is := is.New(t)
//
// 			parsed, err := Parse(tc.input)
// 			if tc.expErr != "" {
// 				is.True(err != nil)              // expected error
// 				is.Equal(tc.expErr, err.Error()) // expected different error
// 			} else {
// 				is.True(err == nil)                  // expected no error
// 				is.Equal(tc.expVal, parsed.ReadTime) // expected different read time
// 			}
// 		})
// 	}
// }
