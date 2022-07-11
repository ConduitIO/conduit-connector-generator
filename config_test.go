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
		"recordCount": "-1",
		"readTime":    "5s",
		"fields":      "id:int,name:string,joined:time,admin:bool",
		"format":      FormatRaw,
	})
	is.NoErr(err)
	is.Equal(int64(-1), underTest.RecordCount)
	is.Equal(5*time.Second, underTest.ReadTime)
	is.Equal(map[string]string{"id": "int", "name": "string", "joined": "time", "admin": "bool"}, underTest.Fields)
	is.Equal(FormatRaw, underTest.Format)
}

func TestParse_PayloadFile_Fields(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		Fields:      "id:int",
		PayloadFile: "/path/to/file.txt",
	})
	is.True(err != nil)
	is.Equal("cannot specify fields and payload field at the same time", err.Error())
}

func TestParse_PayloadFile(t *testing.T) {
	is := is.New(t)
	underTest, err := Parse(map[string]string{
		PayloadFile: "/path/to/file.txt",
	})
	is.NoErr(err)
	is.Equal("/path/to/file.txt", underTest.PayloadFile)
}

func TestParse_FormatStructured_PayloadFile(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"format":      FormatStructured,
		"payloadFile": "/path/to/file.txt",
	})
	is.True(err != nil)
	is.Equal("payload file can only go with raw format", err.Error())
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
	is.Equal(`failed parsing field spec: invalid field spec "abc:"`, err.Error())
}

func TestParseFields_MalformedFields_NameOnly(t *testing.T) {
	is := is.New(t)
	_, err := Parse(map[string]string{
		"fields": "abc",
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
