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

func TestParseFull(t *testing.T) {
	is := is.New(t)
	underTest, err := Parse(map[string]string{
		RecordCount: "-1",
		ReadTime:    "5s",
		Fields:      "id:int,name:string,joined:time,admin:bool",
		Format:      FormatRaw,
	})
	is.NoErr(err)
	is.Equal(int64(-1), underTest.RecordCount)
	is.Equal(5*time.Second, underTest.ReadTime)
	is.Equal(map[string]string{"id": "int", "name": "string", "joined": "time", "admin": "bool"}, underTest.Fields)
	is.Equal(FormatRaw, underTest.Format)
}

func TestParse_PayloadFile(t *testing.T) {
	testCases := []struct {
		name    string
		input   map[string]string
		wantErr string
		wantCfg Config
	}{
		{
			name: "cannot specify fields and payload field at the same time",
			input: map[string]string{
				Fields:      "id:int",
				PayloadFile: "/path/to/file.txt",
			},
			wantErr: "cannot specify fields and payload field at the same time",
		},
		{
			name: "payload file can only go with raw format",
			input: map[string]string{
				Format:        FormatStructured,
				"payloadFile": "/path/to/file.txt",
			},
			wantErr: "payload file can only go with raw format",
		},
		{
			name: "payload file, default format",
			input: map[string]string{
				PayloadFile: "/path/to/file.txt",
			},
			wantCfg: Config{
				RecordCount: -1,
				Format:      FormatRaw,
				PayloadFile: "/path/to/file.txt",
			},
		},
		{
			name: "payload file, raw format",
			input: map[string]string{
				Format:      FormatRaw,
				PayloadFile: "/path/to/file.txt",
			},
			wantCfg: Config{
				RecordCount: -1,
				Format:      FormatRaw,
				PayloadFile: "/path/to/file.txt",
			},
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

func TestParseFields_RequiredNotPresent(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		RecordCount: "100",
		ReadTime:    "5s",
	})
	is.True(err != nil)
	is.Equal("either fields or a payload need to be specified", err.Error())
}

func TestParseFields_OptionalNotPresent(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		Fields: "a:int",
	})
	is.NoErr(err)
}

func TestParseFields_MalformedFields_NoType(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		Fields: "abc:",
	})
	is.True(err != nil)
	is.Equal(`failed parsing field spec: invalid field spec "abc:"`, err.Error())
}

func TestParseFields_MalformedFields_NameOnly(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		Fields: "abc",
	})
	is.True(err != nil)
	is.Equal(`failed parsing field spec: invalid field spec "abc"`, err.Error())
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
				Fields: "id:int",
				Format: FormatRaw,
			},
			expErr: "",
			expVal: FormatRaw,
		},
		{
			name: "parse 'structured'",
			input: map[string]string{
				Fields: "id:int",
				Format: FormatStructured,
			},
			expErr: "",
			expVal: FormatStructured,
		},
		{
			name: "default is 'raw' when no value present",
			input: map[string]string{
				Fields: "id:int",
			},
			expErr: "",
			expVal: FormatRaw,
		},
		{
			name: "default is 'raw' when empty string present",
			input: map[string]string{
				Fields: "id:int",
				Format: "",
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
