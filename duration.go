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
	"errors"
	"fmt"
	"time"
)

type durationHelper struct {
}

// parseNonNegative parses the given duration string and also check if the duration is non-negative.
func (dh durationHelper) parseNonNegative(s string, d time.Duration) (time.Duration, error) {
	return dh.parse(
		s,
		d,
		func(p time.Duration) error {
			if p < 0 {
				return errors.New("duration cannot be negative")
			}
			return nil
		},
	)
}

// parsePositive parses the given duration string and also check if the duration is positive.
func (dh durationHelper) parsePositive(s string, d time.Duration) (time.Duration, error) {
	return dh.parse(
		s,
		d,
		func(p time.Duration) error {
			if p <= 0 {
				return errors.New("duration must be positive")
			}
			return nil
		},
	)
}

// parse parses the given duration string and (if successful) runs the provided check on the parsed duration.
func (dh durationHelper) parse(s string, d time.Duration, check func(duration time.Duration) error) (time.Duration, error) {
	var out time.Duration
	if s == "" {
		out = d
	} else {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("duration cannot be parsed: %w", err)
		}
		out = parsed
	}

	if err := check(out); err != nil {
		return time.Duration(0), err
	}
	return out, nil
}
