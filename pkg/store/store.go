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
	// IntentGetAll streams all intents not in excludeIntentNames to intentChan and sends at most one error to errChan.
	// It always closes both channels before returning, errChan first: once intentChan is closed, errChan is closed
	// too and can be drained without blocking to pick up a buffered error.
	// Sends are abandoned once ctx is done, so a cancelled stream may end without an error; check ctx.Err().
	IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error)
	IntentDelete(ctx context.Context, intentName string, IgnoreNonExisting bool) error
	// Delete the Store. Implicitly closes store
	Delete() error
	// Close the Store
	Close() error
}
