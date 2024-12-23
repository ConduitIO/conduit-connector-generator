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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-generator/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"golang.org/x/time/rate"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource

	config      Config
	recordCount int
	burstUntil  time.Time

	recordGenerator internal.RecordGenerator
	rateLimiter     *rate.Limiter
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{})
}

func (s *Source) Open(_ context.Context, _ opencdc.Position) error {
	var generators []internal.RecordGenerator
	for collection, cfg := range s.config.GetCollectionConfigs() {
		var gen internal.RecordGenerator
		var err error
		switch cfg.Format.Type {
		case FormatTypeFile:
			gen, err = internal.NewFileRecordGenerator(collection, cfg.SdkOperations(), cfg.Format.FileOptionsPath)
		case FormatTypeRaw:
			gen, err = internal.NewRawRecordGenerator(collection, cfg.SdkOperations(), cfg.Format.Options)
		case FormatTypeStructured:
			gen, err = internal.NewStructuredRecordGenerator(collection, cfg.SdkOperations(), cfg.Format.Options)
		}
		if err != nil {
			return fmt.Errorf("failed to create record generator for collection %q: %w", collection, err)
		}
		generators = append(generators, gen)
	}

	s.recordGenerator = internal.Combine(generators...)
	if rl := s.config.RateLimit(); rl > 0 {
		s.rateLimiter = rate.NewLimiter(rl, 1)
	}
	if s.config.Burst.SleepTime > 0 {
		s.burstUntil = time.Now().Add(s.config.Burst.GenerateTime)
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	if ctx.Err() != nil {
		// stop producing new records if context is canceled
		return opencdc.Record{}, ctx.Err()
	}

	if s.config.RecordCount > 0 && s.recordCount >= s.config.RecordCount {
		// nothing more to produce, block until context is done
		<-ctx.Done()
		return opencdc.Record{}, ctx.Err()
	}

	// prepare next record in advance to avoid losing time in case of rate limiting
	rec := s.recordGenerator.Next()

	// bursts
	if s.config.Burst.SleepTime > 0 {
		err := s.sleepBetweenBursts(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
	}

	// rate limiting
	if s.rateLimiter != nil {
		err := s.rateLimiter.Wait(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
	}

	s.recordCount++
	return rec, nil
}

func (s *Source) sleepBetweenBursts(ctx context.Context) error {
	now := time.Now()
	if now.Before(s.burstUntil) {
		return nil // no sleep needed
	}

	// Adjust the next burst time until it's in the future.
	for s.burstUntil.Before(now) {
		s.burstUntil = s.burstUntil.Add(s.config.Burst.SleepTime + s.config.Burst.GenerateTime)
	}

	// Check if we are in the sleep phase.
	wakeAt := s.burstUntil.Add(-s.config.Burst.GenerateTime)
	dur := wakeAt.Sub(now)
	if dur < 0 {
		// We are in the generating phase, no need to sleep.
		return nil
	}

	// Block until the next burst window or context is done.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(dur):
		return nil
	}
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil // no ack needed
}

func (s *Source) Teardown(_ context.Context) error {
	return nil // nothing to stop
}
