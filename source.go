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

	"github.com/conduitio/conduit-connector-generator/internal"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource

	created       int
	config        Config
	generateUntil time.Time

	recordGenerator internal.RecordGenerator
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

func (s *Source) Configure(_ context.Context, config map[string]string) error {
	err := sdk.Util.ParseConfig(config, &s.config)
	if err != nil {
		return err
	}
	return s.config.Validate()
}

func (s *Source) Open(_ context.Context, _ sdk.Position) error {
	var generators []internal.RecordGenerator
	for collection, cfg := range s.config.GetConfigCollections() {
		var gen internal.RecordGenerator
		var err error
		switch cfg.Format.Type {
		case FormatTypeFile:
			gen, err = internal.NewFileRecordGenerator(collection, cfg.SdkOperation(), cfg.Format.FileOptionsPath)
		case FormatTypeRaw:
			gen, err = internal.NewRawRecordGenerator(collection, cfg.SdkOperation(), cfg.Format.Options)
		case FormatTypeStructured:
			gen, err = internal.NewStructuredRecordGenerator(collection, cfg.SdkOperation(), cfg.Format.Options)
		}
		if err != nil {
			return fmt.Errorf("failed to create record generator for collection %q: %w", collection, err)
		}
		generators = append(generators, gen)
	}
	s.recordGenerator = internal.Combine(generators...)
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if ctx.Err() != nil {
		// stop producing new records if context is canceled
		return sdk.Record{}, ctx.Err()
	}

	if s.created >= s.config.RecordCount && s.config.RecordCount >= 0 {
		// nothing more to produce, block until context is done
		<-ctx.Done()
		return sdk.Record{}, ctx.Err()
	}
	s.created++

	// TODO use rate limiting instead of manual sleeps

	if s.shouldSleep() {
		err := s.sleep(ctx, s.config.Burst.SleepTime)
		if err != nil {
			return sdk.Record{}, err
		}
		s.generateUntil = time.Now().Add(s.config.Burst.GenerateTime)
	}

	err := s.sleep(ctx, s.config.ReadTime)
	if err != nil {
		return sdk.Record{}, err
	}

	return s.recordGenerator.Next(), nil
}

func (s *Source) shouldSleep() bool {
	if s.config.Burst.SleepTime == 0 {
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
