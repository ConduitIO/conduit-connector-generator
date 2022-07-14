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
		"recordCount": "-1",
		"readTime":    "5s",
		"fields":      "id:int,name:string,joined:time,admin:bool",
	})
	is.NoErr(err)
	is.Equal(int64(-1), underTest.RecordCount)
	is.Equal(5*time.Second, underTest.ReadTime)
	is.Equal(map[string]string{"id": "int", "name": "string", "joined": "time", "admin": "bool"}, underTest.Fields)
}

func TestParseFields_RequiredNotPresent(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"recordCount": "100",
		"readTime":    "5s",
	})
	is.True(err != nil)
	is.Equal("no fields specified", err.Error())
}

func TestParseFields_OptionalNotPresent(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"fields": "a:int",
	})
	is.NoErr(err)
}

func TestParseFields_MalformedFields_NoType(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"fields": "abc:",
	})
	is.True(err != nil)
	is.Equal(`invalid field spec "abc:"`, err.Error())
}

func TestParseFields_MalformedFields_NameOnly(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"fields": "abc",
	})
	is.True(err != nil)
	is.Equal(`invalid field spec "abc"`, err.Error())
}

func TestParseFormat(t *testing.T) {
	testCases := []struct {
		name   string
		input  map[string]string
		expErr string
		expVal string
	}{
		{
			name: "parse 'raw'",
			input: map[string]string{
				"fields": "id:int",
				"format": FormatRaw,
			},
			expErr: "",
			expVal: FormatRaw,
		},
		{
			name: "parse 'structured'",
			input: map[string]string{
				"fields": "id:int",
				"format": FormatStructured,
			},
			expErr: "",
			expVal: FormatStructured,
		},
		{
			name: "default is 'raw' when no value present",
			input: map[string]string{
				"fields": "id:int",
			},
			expErr: "",
			expVal: FormatRaw,
		},
		{
			name: "default is 'raw' when empty string present",
			input: map[string]string{
				"fields": "id:int",
				"format": "",
			},
			expErr: "",
			expVal: FormatRaw,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			parsed, err := Parse(tc.input)
			if tc.expErr != "" {
				is.True(err != nil)
				is.Equal(tc.expErr, err.Error())
			} else {
				is.True(err == nil)
				is.Equal(tc.expVal, parsed.Format)
			}
		})
	}
}

func TestParse_Durations(t *testing.T) {
	testCases := []fieldTest[time.Duration]{
		{
			name: "default read time is 0s",
			input: map[string]string{
				Fields: "id:int",
			},
			expErr: "",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
		},
		{
			name: "negative read time not allowed",
			input: map[string]string{
				Fields:   "id:int",
				ReadTime: "-1s",
			},
			expErr: "invalid read time: duration cannot be negative",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.ReadTime },
		},
		{
			name: "default sleep time is 0s",
			input: map[string]string{
				Fields: "id:int",
			},
			expErr: "",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
		},
		{
			name: "negative sleep time not allowed",
			input: map[string]string{
				Fields:    "id:int",
				SleepTime: "-1s",
			},
			expErr: "invalid sleep time: duration cannot be negative",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.SleepTime },
		},
		{
			name: "negative generate time not allowed",
			input: map[string]string{
				Fields:       "id:int",
				GenerateTime: "-1s",
			},
			expErr: "invalid generate time: duration must be positive",
			expVal: time.Duration(0),
			getter: func(cfg Config) time.Duration { return cfg.GenerateTime },
		},
		{
			name: "generate time 0 not allowed",
			input: map[string]string{
				Fields:       "id:int",
				GenerateTime: "0ms",
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
