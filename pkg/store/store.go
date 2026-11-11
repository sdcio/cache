// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"fmt"

	"github.com/sdcio/cache/pkg/types"
)

type Store interface {
	IntentModify(ctx context.Context, intentName string, data []byte) error
	IntentExists(ctx context.Context, intentName string) (bool, error)
	IntentsList(ctx context.Context) ([]string, error)
	IntentGet(ctx context.Context, intentName string) ([]byte, error)
	// IntentGetAll streams a consistent snapshot of all intents not in excludeIntentNames to intentChan.
	// It sends at most one error and closes both channels before returning, errChan first, so once intentChan
	// is closed errChan can be drained without blocking. If errChan has capacity, the error (including a
	// cancellation wrapping ctx.Err()) is always delivered; an unbuffered errChan loses it once ctx is done.
	IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error)
	IntentDelete(ctx context.Context, intentName string, IgnoreNonExisting bool) error
	// Delete the Store. Implicitly closes store
	Delete() error
	// Close the Store
	Close() error
}

// StreamIntents delivers an IntentGetAll result (a snapshot or the error taking it) per the IntentGetAll contract.
func StreamIntents(ctx context.Context, intents []*types.Intent, err error, intentChan chan<- *types.Intent, errChan chan<- error) {
	defer close(intentChan)
	defer close(errChan)

	if err == nil {
		err = sendIntents(ctx, intents, intentChan)
	}
	if err == nil {
		return
	}
	// Try a non-blocking send first so a buffered or waiting errChan gets the error even when ctx is done.
	select {
	case errChan <- err:
		return
	default:
	}
	select {
	case errChan <- err:
	case <-ctx.Done():
	}
}

func sendIntents(ctx context.Context, intents []*types.Intent, intentChan chan<- *types.Intent) error {
	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("streaming canceled: %w", err)
		}
		select {
		case intentChan <- intent:
		case <-ctx.Done():
			return fmt.Errorf("streaming canceled: %w", ctx.Err())
		}
	}
	return nil
}
