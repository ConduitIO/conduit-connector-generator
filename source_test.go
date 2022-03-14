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
	"context"
	"encoding/json"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestRead_RawData(t *testing.T) {
	is := is.New(t)
	cfg := map[string]string{
		"recordCount": "1",
		"fields":      "id:int,name:string,joined:time,admin:bool",
		"format":      Raw,
	}
	underTest := NewSource()
	t.Cleanup(func() {
		_ = underTest.Teardown(context.Background())
	})

	err := underTest.Configure(context.Background(), cfg)
	is.NoErr(err)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.(sdk.RawData)
	is.True(ok)

	recStruct := struct {
		id     int32
		name   string
		joined time.Time
		admin  bool
	}{}
	err = json.Unmarshal(v, &recStruct) //nolint:staticcheck // test struct
	is.NoErr(err)
}

func TestRead_StructuredData(t *testing.T) {
	is := is.New(t)
	cfg := map[string]string{
		"recordCount": "1",
		"fields":      "id:int,name:string,joined:time,admin:bool",
		"format":      Structured,
	}
	underTest := NewSource()
	t.Cleanup(func() {
		_ = underTest.Teardown(context.Background())
	})

	err := underTest.Configure(context.Background(), cfg)
	is.NoErr(err)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.(sdk.StructuredData)
	is.True(ok)

	recStruct := struct {
		id     int32
		name   string
		joined time.Time
		admin  bool
	}{}
	// map to json to struct, so we can check types of all fields easily
	bytes, err := json.Marshal(v)
	is.NoErr(err)
	err = json.Unmarshal(bytes, &recStruct) //nolint:staticcheck // test struct
	is.NoErr(err)
}
