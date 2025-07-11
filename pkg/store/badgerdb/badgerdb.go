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

package badgerdb

import (
	"context"
	"fmt"
	"os"
	"path"
	"slices"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

type TypePrefix []byte

var (
	TypePrefixIntent TypePrefix = []byte{0x00}
)

type badgerDB struct {
	db   *badger.DB
	path string
}

func NewBadgerDBStore(path string) (*badgerDB, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &badgerDB{
		db:   db,
		path: path,
	}, nil
}

func (b *badgerDB) IntentDelete(ctx context.Context, intentName string, IgnoreNonExisting bool) error {
	// Start a writable transaction.
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	intentNameByte := append(TypePrefixIntent, []byte(intentName)...)

	// Delete the intent
	err := txn.Delete(intentNameByte)
	if err != nil {
		if IgnoreNonExisting {
			return nil
		}
		return err
	}

	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (b *badgerDB) IntentModify(ctx context.Context, intentName string, data []byte) error {
	// Start a writable transaction.
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	intentNameByte := append(TypePrefixIntent, []byte(intentName)...)

	// Set the data
	err := txn.Set(intentNameByte, data)
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (b *badgerDB) IntentExists(ctx context.Context, intentName string) (bool, error) {
	list, err := b.IntentsList(ctx)
	if err != nil {
		return false, err
	}
	return slices.Contains(list, intentName), nil
}

func (b *badgerDB) IntentsList(ctx context.Context) ([]string, error) {
	result := []string{}
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(TypePrefixIntent); it.Next() {
			item := it.Item()
			k := item.Key()
			result = append(result, string(k[1:]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *badgerDB) IntentGet(ctx context.Context, intentName string) ([]byte, error) {
	var result []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append(TypePrefixIntent, []byte(intentName)...))
		if err != nil {
			return err
		}

		result, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *badgerDB) IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	defer close(intentChan)
	defer close(errChan)

	intentNames, err := b.IntentsList(ctx)
	if err != nil {
		errChan <- err
		return
	}

	for _, intentName := range intentNames {
		if slices.Contains(excludeIntentNames, intentName) {
			continue
		}
		select {
		case <-ctx.Done(): // Check for cancellation or timeout
			errChan <- fmt.Errorf("streaming canceled: %v", ctx.Err())
			return
		default:
			data, err := b.IntentGet(ctx, intentName)
			if err != nil {
				errChan <- err
				return
			}
			intentChan <- types.NewIntent(intentName, data)
		}
	}
}

func (b *badgerDB) Close() error {
	return b.db.Close()
}

func (b *badgerDB) Delete() error {
	err := b.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(b.path)
}

func PreconfigureBadgerDbInitFunc(basepath string) (func(cachename string) (store.Store, error), error) {

	_, err := os.Stat(basepath)
	if err != nil {
		err := os.MkdirAll(basepath, 0755)
		if err != nil {
			return nil, err
		}
	}

	return func(cacheName string) (store.Store, error) {
		return NewBadgerDBStore(path.Join(basepath, cacheName))
	}, nil
}
