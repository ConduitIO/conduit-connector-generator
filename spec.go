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
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process (i.e. the Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "generator",
		Summary: "A plugin capable of generating dummy records (in different formats).",
		Description: "The generator plugin generates test data for Conduit pipelines. It can return data from a file, or" +
			" generate dummy data. The generated dummy data can be returned in raw of structured format.",
		Version: version,
		Author:  "Meroxa, Inc.",
	}
}
