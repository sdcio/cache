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
	IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error)
	IntentDelete(ctx context.Context, intentName string) error
	// Delete the Store. Implicitly closes store
	Delete() error
	// Close the Store
	Close() error
}
