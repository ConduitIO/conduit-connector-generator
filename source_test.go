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
	"maps"
	"os"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/goccy/go-json"
	"github.com/matryer/is"
)

func TestSource_Read_RawData(t *testing.T) {
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
	now := time.Now()

	v, ok := rec.Payload.Before.(sdk.RawData)
	is.True(ok)

	recMap := make(map[string]any)
	err = json.Unmarshal(v, &recMap)
	is.NoErr(err)

	is.Equal(len(recMap), 4)
	is.True(recMap["id"].(float64) > 0)
	is.True(recMap["name"].(string) != "")
	_, ok = recMap["admin"].(bool)
	is.True(ok)

	ts := recMap["joined"].(float64)
	joined := time.Unix(0, int64(ts))
	is.True(!joined.After(now))
	is.True(joined.After(now.Add(-time.Millisecond * 10)))
}

func TestSource_Read_PayloadFile(t *testing.T) {
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

func TestSource_Read_StructuredData(t *testing.T) {
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
	now := time.Now()

	v, ok := rec.Payload.After.(sdk.StructuredData)
	is.True(ok)

	is.Equal(len(v), 4)
	is.True(v["id"].(int) > 0)
	is.True(v["name"].(string) != "")
	_, ok = v["admin"].(bool)
	is.True(ok)

	joined := time.Unix(0, v["joined"].(int64))
	is.True(!joined.After(now))
	is.True(joined.After(now.Add(-time.Millisecond * 10)))
}

func TestSource_Read_RateLimit(t *testing.T) {
	cfg := map[string]string{
		"burst.sleepTime":    "100ms",
		"burst.generateTime": "150ms",
		"format.type":        "raw",
		"format.options.id":  "int",
		"operation":          "create,update",
	}

	// Test rate parameter
	t.Run("parameter-rate", func(t *testing.T) {
		cfg := maps.Clone(cfg)
		cfg["rate"] = "20"
		testSourceRateLimit(t, cfg)
	})
	// Test readTime parameter
	t.Run("parameter-readTime", func(t *testing.T) {
		cfg := maps.Clone(cfg)
		cfg["readTime"] = "50ms"
		testSourceRateLimit(t, cfg)
	})
}

func testSourceRateLimit(t *testing.T, cfg map[string]string) {
	ctx := context.Background()

	underTest := openTestSource(t, cfg)

	const epsilon = time.Millisecond * 10
	readAssertDelay := func(is *is.I, expectedDelay time.Duration) {
		is.Helper()
		start := time.Now()
		_, err := underTest.Read(ctx)
		dur := time.Since(start)
		is.NoErr(err)
		is.True(dur >= expectedDelay-epsilon) // expected longer delay
		is.True(dur <= expectedDelay+epsilon) // expected shorter delay
	}

	is := is.New(t)

	// We start in the generate cycle, we can test the rate limiting here.
	// The first record should be read immediately.
	readAssertDelay(is, 0)

	// The second record should already be rate limited and delayed by 50ms.
	readAssertDelay(is, 50*time.Millisecond)

	// If we wait for 50ms before reading, the next record should be read immediately.
	time.Sleep(50 * time.Millisecond)
	readAssertDelay(is, 0)

	// If we wait for 25ms, the next record should be read after 25ms.
	time.Sleep(25 * time.Millisecond)
	readAssertDelay(is, 25*time.Millisecond)

	// By now we should have reached the end of burst.generateTime (150ms).
	// If we try to read a record now we should have to wait for 100ms (burst.sleepTime).
	readAssertDelay(is, 100*time.Millisecond)

	// After the sleep cycle we are again in the generate cycle. Reading a record
	// should have the normal delay of 50ms.
	readAssertDelay(is, 50*time.Millisecond)

	// Wait for 100ms (remaining generate time) + 50ms (half of sleep time) = 150ms,
	// so we are in the middle of the sleep cycle. Reading at that point should
	// take 50ms.
	time.Sleep(150 * time.Millisecond)
	readAssertDelay(is, 50*time.Millisecond)
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
