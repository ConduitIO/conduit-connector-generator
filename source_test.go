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
	"os"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestRead_RawData(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":           "1",
			"format.type":           "raw",
			"format.options.id":     "int",
			"format.options.name":   "string",
			"format.options.joined": "time",
			"format.options.admin":  "bool",
			"operation":             "delete",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.Before.(sdk.RawData)
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

func TestRead_PayloadFile(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":         "1",
			"format.type":         "file",
			"format.options.path": "./source_test.go",
			"operation":           "update",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.After.(sdk.RawData)
	is.True(ok)

	expected, err := os.ReadFile("./source_test.go")
	is.NoErr(err)
	is.Equal(expected, v.Bytes())
}

func TestRead_StructuredData(t *testing.T) {
	is := is.New(t)
	underTest := openTestSource(
		t,
		map[string]string{
			"recordCount":           "1",
			"format.type":           "structured",
			"format.options.id":     "int",
			"format.options.name":   "string",
			"format.options.joined": "time",
			"format.options.admin":  "bool",
			"operation":             "snapshot",
		},
	)

	rec, err := underTest.Read(context.Background())
	is.NoErr(err)

	v, ok := rec.Payload.After.(sdk.StructuredData)
	is.True(ok)

	is.Equal(len(v), 4)
	is.True(v["id"].(int) > 0)
	is.True(v["name"].(string) != "")
	// is.True(v["joined"].) TODO joined
	_, ok = v["admin"].(bool)
	is.True(ok)
}

func TestSource_Read_SleepGenerate(t *testing.T) {
	t.Skip("needs to be rewritten")
	is := is.New(t)

	underTest := openTestSource(
		t,
		map[string]string{
			"readTime":           "10ms",
			"burst.sleepTime":    "200ms",
			"burst.generateTime": "50ms",
			"format.type":        "raw",
			"format.options.id":  "int",
			"operation":          "create,update",
		},
	)

	type result struct {
		err      error
		duration time.Duration
	}

	// first read: sleep time + read time + bit of buffer
	results := make(chan result)
	go func() {
		start := time.Now()
		_, err := underTest.Read(context.Background())

		results <- result{err: err, duration: time.Since(start)}
	}()

	select {
	case r := <-results:
		is.NoErr(r.err)
		is.True(r.duration >= 210*time.Millisecond) // expected source to sleep for given time
	case <-time.After(220 * time.Millisecond):
		is.Fail() // timed out waiting for record
	}

	// we have 40ms left for generating new records
	// (50ms total, minus the 10ms for the first record)
	// so we read 4 more records here
	results = make(chan result)
	go func() {
		start := time.Now()
		var err error
		for i := 1; i <= 4; i++ {
			_, err = underTest.Read(context.Background())
			is.NoErr(err)
		}

		results <- result{err: err, duration: time.Since(start)}
	}()

	select {
	case r := <-results:
		is.NoErr(r.err)
		is.True(r.duration >= 40*time.Millisecond) // expected source to sleep for given time
	case <-time.After(50 * time.Millisecond):
		is.Fail() // timed out waiting for record
	}

	// one more read, to verify that we got through another sleep cycle,
	// and started generating records again
	// sleep time + read time + bit of buffer
	results = make(chan result)
	go func() {
		start := time.Now()
		_, err := underTest.Read(context.Background())

		results <- result{err: err, duration: time.Since(start)}
	}()

	select {
	case r := <-results:
		is.NoErr(r.err)
		is.True(r.duration >= 210*time.Millisecond) // expected source to sleep for given time
	case <-time.After(220 * time.Millisecond):
		is.Fail() // timed out waiting for record
	}
}

func openTestSource(t *testing.T, cfg map[string]string) sdk.Source {
	is := is.New(t)

	s := NewSource()
	t.Cleanup(func() {
		_ = s.Teardown(context.Background())
	})

	err := s.Configure(context.Background(), cfg)
	is.NoErr(err)

	err = s.Open(context.Background(), nil)
	is.NoErr(err)

	return s
}
