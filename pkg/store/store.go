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

	"github.com/sdcio/cache/pkg/types"
)

type Store interface {
	IntentModify(ctx context.Context, intentName string, data []byte) error
	IntentExists(ctx context.Context, intentName string) (bool, error)
	IntentsList(ctx context.Context) ([]string, error)
	IntentGet(ctx context.Context, intentName string) ([]byte, error)
	// IntentGetAll streams a consistent view of all intents not in excludeIntentNames to intentChan.
	// Every wait also selects on ctx and on the store closing, so it returns once either happens.
	// It sends at most one error and closes both channels before returning, errChan first. A buffered
	// errChan always receives the error, including a cancellation wrapping ctx.Err().
	IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error)
	IntentDelete(ctx context.Context, intentName string, IgnoreNonExisting bool) error
	// Delete the Store. Implicitly closes store
	Delete() error
	// Close the Store
	Close() error
}

// FinishIntentStream ends an IntentGetAll stream: it delivers err, if any, and closes both channels.
// done is the store's close signal; nil if there is none.
func FinishIntentStream(ctx context.Context, done <-chan struct{}, err error, intentChan chan<- *types.Intent, errChan chan<- error) {
	defer close(intentChan)
	defer close(errChan)
	if err == nil {
		return
	}
	// A buffered or waiting errChan gets the error even if ctx is already done.
	select {
	case errChan <- err:
		return
	default:
	}
	select {
	case errChan <- err:
	case <-ctx.Done():
	case <-done:
	}
}
