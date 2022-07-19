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
	"time"

	"github.com/matryer/is"
)

type fieldTest[T interface{}] struct {
	name   string
	input  map[string]string
	expErr string
	expVal T
	getter func(Config) T
}

func TestParseFull(t *testing.T) {
	is := is.New(t)
	underTest, err := Parse(map[string]string{
		RecordCount:   "-1",
		ReadTime:      "5s",
		FormatType:    FormatRaw,
		FormatOptions: "id:int,name:string,joined:time,admin:bool",
	})
	is.NoErr(err)
	is.Equal(int64(-1), underTest.RecordCount)
	is.Equal(5*time.Second, underTest.ReadTime)
	is.Equal(
		map[string]string{"id": "int", "name": "string", "joined": "time", "admin": "bool"},
		underTest.RecordConfig.FormatOptions["fields"],
	)
	is.Equal(FormatRaw, underTest.RecordConfig.FormatType)
}

func TestParseFields_RequiredNotPresent(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		RecordCount: "100",
		ReadTime:    "5s",
	})
	is.True(err != nil)
	is.Equal("required parameters missing [format.type format.options]", err.Error())
}

func TestParse_DifferentFormats(t *testing.T) {
	testCases := []struct {
		name    string
		input   map[string]string
		wantErr string
		wantCfg Config
	}{
		{
			name: "raw format",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
			},
			wantErr: "",
			wantCfg: Config{
				RecordCount: -1,
				ReadTime:    0,
				RecordConfig: RecordConfig{
					FormatType: FormatRaw,
					FormatOptions: map[string]interface{}{
						"fields": map[string]string{"id": "int"},
					},
				},
			},
		},
		{
			name: "structured format",
			input: map[string]string{
				FormatType:    FormatStructured,
				FormatOptions: "id:int",
			},
			wantErr: "",
			wantCfg: Config{
				RecordCount: -1,
				ReadTime:    0,
				RecordConfig: RecordConfig{
					FormatType: FormatStructured,
					FormatOptions: map[string]interface{}{
						"fields": map[string]string{"id": "int"},
					},
				},
			},
		},
		{
			name: "file format",
			input: map[string]string{
				FormatType:    FormatFile,
				FormatOptions: "/path/to/file.txt",
			},
			wantErr: "",
			wantCfg: Config{
				RecordCount: -1,
				ReadTime:    0,
				RecordConfig: RecordConfig{
					FormatType: FormatFile,
					FormatOptions: map[string]interface{}{
						"path": "/path/to/file.txt",
					},
				},
			},
		},
		{
			name: "file format, no path",
			input: map[string]string{
				FormatType:    FormatFile,
				FormatOptions: "",
			},
			wantErr: "failed configuring payload generator: file path not specified",
			wantCfg: Config{},
		},
		{
			name: "structured, malformed fields, no type",
			input: map[string]string{
				FormatType:    FormatStructured,
				FormatOptions: "abc:",
			},
			wantErr: `failed configuring payload generator: failed parsing fields: invalid field spec "abc:"`,
			wantCfg: Config{},
		},
		{
			name: "structured, malformed fields, name only",
			input: map[string]string{
				FormatType:    FormatStructured,
				FormatOptions: "abc",
			},
			wantErr: `failed configuring payload generator: failed parsing fields: invalid field spec "abc"`,
			wantCfg: Config{},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			cfg, err := Parse(tc.input)
			if tc.wantErr != "" {
				is.True(err != nil)
				is.Equal(tc.wantErr, err.Error())
			} else {
				is.NoErr(err)
				is.Equal(tc.wantCfg, cfg)
			}
		})
	}
}

func TestParse_Durations(t *testing.T) {
	testCases := []fieldTest[time.Duration]{
		{
			name: "default read time is 0s",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
			},
			expErr: "",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
		},
		{
			name: "negative read time not allowed",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
				ReadTime:      "-1s",
			},
			expErr: "invalid read time: duration cannot be negative",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
		},
		{
			name: "default sleep time is 0s",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
			},
			expErr: "",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
		},
		{
			name: "negative sleep time not allowed",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
				SleepTime:     "-1s",
			},
			expErr: "invalid sleep time: duration cannot be negative",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
		},
		{
			name: "negative generate time not allowed",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
				GenerateTime:  "-1s",
			},
			expErr: "invalid generate time: duration must be positive",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.GenerateTime },
		},
		{
			name: "generate time 0 not allowed",
			input: map[string]string{
				FormatType:    FormatRaw,
				FormatOptions: "id:int",
				GenerateTime:  "0ms",
			},
			expErr: "invalid generate time: duration must be positive",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.GenerateTime },
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			parsed, err := Parse(tc.input)
			if tc.expErr != "" {
				is.True(err != nil)              // expected error
				is.Equal(tc.expErr, err.Error()) // expected different error
			} else {
				is.True(err == nil)                  // expected no error
				is.Equal(tc.expVal, parsed.ReadTime) // expected different read time
			}
		})
	}
}
