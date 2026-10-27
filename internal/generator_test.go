// Copyright © 2024 Meroxa, Inc.
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

package internal

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestFileRecordGenerator_RecordsOwnTheirPayload(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "payload.txt")
	is.NoErr(os.WriteFile(path, []byte("hello"), 0o600))

	g, err := NewFileRecordGenerator("c", []opencdc.Operation{opencdc.OperationCreate}, path)
	is.NoErr(err)

	r1, r2 := g.Next(), g.Next()
	r1.Payload.After.(opencdc.RawData)[0] = 'J'

	is.Equal(string(r1.Payload.After.Bytes()), "Jello")
	is.Equal(string(r2.Payload.After.Bytes()), "hello") // mutating record 1 must not change record 2
	is.Equal(string(g.Next().Payload.After.Bytes()), "hello")
}

func TestFileRecordGenerator_UpdateBeforeAndAfterAreIndependent(t *testing.T) {
	is := is.New(t)
	path := filepath.Join(t.TempDir(), "payload.txt")
	is.NoErr(os.WriteFile(path, []byte("hello"), 0o600))

	g, err := NewFileRecordGenerator("c", []opencdc.Operation{opencdc.OperationUpdate}, path)
	is.NoErr(err)

	r := g.Next()
	r.Payload.Before.(opencdc.RawData)[0] = 'J'
	is.Equal(string(r.Payload.After.Bytes()), "hello") // Before and After must not alias
}

func TestCombine_PositionsAreUniqueAcrossCollections(t *testing.T) {
	// 12 or more generators is where the old "<index><count>" format collided
	// (generator 1 at count 11 and generator 11 at count 1 are both "111").
	for _, n := range []int{2, 12, 30} {
		t.Run(strconv.Itoa(n)+" collections", func(t *testing.T) {
			is := is.New(t)
			var gens []RecordGenerator
			for i := 0; i < n; i++ {
				g, err := NewStructuredRecordGenerator(
					"c"+strconv.Itoa(i),
					[]opencdc.Operation{opencdc.OperationCreate},
					intFields(),
				)
				is.NoErr(err)
				gens = append(gens, g)
			}
			c := Combine(gens...)

			seen := map[string]int{}
			for i := 0; i < 20000; i++ {
				pos := string(c.Next().Position)
				if prev, dup := seen[pos]; dup {
					t.Fatalf("records %d and %d share position %q", prev, i, pos)
				}
				seen[pos] = i
			}
		})
	}
}

func TestCombine_PositionCollisionPair(t *testing.T) {
	// Deterministic version of the collision: drive generators 1 and 11
	// directly so the test does not depend on the random pick.
	is := is.New(t)
	var gens []RecordGenerator
	for i := 0; i < 12; i++ {
		g, err := NewStructuredRecordGenerator("c", []opencdc.Operation{opencdc.OperationCreate}, intFields())
		is.NoErr(err)
		gens = append(gens, g)
	}
	c := &combinedRecordGenerator{generators: gens}
	pos := func(i int) string { return string(c.nextFrom(i).Position) }

	for n := 0; n < 10; n++ {
		pos(1) // advance generator 1 to count 10
	}
	a := pos(1)  // generator 1, count 11
	b := pos(11) // generator 11, count 1
	is.True(a != b)
}

func intFields() map[string]string { return map[string]string{"a": KnownTypes[0]} }
