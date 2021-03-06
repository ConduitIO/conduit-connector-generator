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
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource

	created         int64
	config          Config
	generateUntil   time.Time
	recordGenerator *recordGenerator
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(_ context.Context, config map[string]string) error {
	parsedCfg, err := Parse(config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	s.config = parsedCfg
	return nil
}

func (s *Source) Open(_ context.Context, _ sdk.Position) error {
	s.recordGenerator = newRecordGenerator(s.config.RecordConfig)
	return s.recordGenerator.init()
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if s.created >= s.config.RecordCount && s.config.RecordCount >= 0 {
		// nothing more to produce, block until context is done
		<-ctx.Done()
		return sdk.Record{}, ctx.Err()
	}
	s.created++

	if s.shouldSleep() {
		err := s.sleep(ctx, s.config.SleepTime)
		if err != nil {
			return sdk.Record{}, err
		}
		s.generateUntil = time.Now().Add(s.config.GenerateTime)
	}

	err := s.sleep(ctx, s.config.ReadTime)
	if err != nil {
		return sdk.Record{}, err
	}

	rec, err := s.recordGenerator.generate()
	if err != nil {
		return sdk.Record{}, err
	}
	return rec, nil
}

func (s *Source) shouldSleep() bool {
	if s.config.SleepTime == 0 {
		return false
	}
	return time.Now().After(s.generateUntil)
}

func (s *Source) sleep(ctx context.Context, duration time.Duration) error {
	if duration > 0 {
		// If a sleep duration is requested the function will block for that
		// period or until the context gets cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(duration):
			return nil
		}
	}

	// By default, we just check if the context is still valid.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil // no ack needed
}

func (s *Source) Teardown(_ context.Context) error {
	return nil // nothing to stop
}
