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
	"math/rand"
	"strconv"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Combine combines multiple record generators into one. It will randomly
// select one of the generators to generate the next record.
func Combine(generators ...RecordGenerator) RecordGenerator {
	if len(generators) == 1 {
		return generators[0]
	}
	return &combinedRecordGenerator{
		generators: generators,
	}
}

type combinedRecordGenerator struct {
	generators []RecordGenerator
}

func (g *combinedRecordGenerator) Next() opencdc.Record {
	i := rand.Intn(len(g.generators))
	gen := g.generators[i]
	rec := gen.Next()

	// keep position unique
	prefix := []byte(strconv.Itoa(i))
	newPos := make([]byte, len(prefix)+len(rec.Position))
	copy(newPos, prefix)
	copy(newPos[len(prefix):], rec.Position)
	rec.Position = newPos

	return rec
}
