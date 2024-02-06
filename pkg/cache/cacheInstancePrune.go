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

package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type pruneData struct {
	pm         *sync.RWMutex
	pruneID    string
	pruneIndex uint8
	createdAt  time.Time
}

func (pd *pruneData) String() string {
	return fmt.Sprintf("pruneID=%s, pruneIndex=%d, createdAt=%s", pd.pruneID, pd.pruneIndex, pd.createdAt)
}

func (ci *cacheInstance) createPruneID(ctx context.Context, force bool) (string, error) {
	log.Debugf("createPruneID: force=%v", force)
	log.Debugf("createPruneID1: prunedata=%+v", ci.prune)
	now := time.Now()
	if ci.prune.pruneID != "" {
		if !now.After(ci.prune.createdAt.Add(ci.cfg.PruneIDLifetime)) && !force {
			return "", errors.New("there is an already ongoing prune transaction")
		}
	}
	// increment current prune index
	ci.prune.pruneIndex++
	log.Debugf("createPruneID2: prunedata=%+v", ci.prune)
	// persist prune index
	err := ci.store.SetPruneIndex(ctx, ci.cfg.Name, ci.prune.pruneIndex)
	if err != nil {
		return "", err
	}
	// TODO: consider other id generation methods
	// https://blog.kowalczyk.info/article/JyRZ/generating-good-unique-ids-in-go.html
	ci.prune.pruneID = xid.New().String()
	ci.prune.createdAt = now
	log.Debugf("createPruneID3: prunedata=%+v", ci.prune)
	return ci.prune.pruneID, nil
}

func (ci *cacheInstance) applyPrune(ctx context.Context, id string) error {
	if ci.prune.pruneID == "" || id != ci.prune.pruneID {
		return errors.New("unknown prune transaction id")
	}
	log.Debugf("applyPrune: prunedata=%+v", ci.prune)
	err := ci.store.Prune(ctx, ci.cfg.Name, "config", ci.prune.pruneIndex)
	if err != nil {
		return err
	}
	err = ci.store.Prune(ctx, ci.cfg.Name, "state", ci.prune.pruneIndex)
	if err != nil {
		return err
	}
	ci.prune.pruneID = ""
	ci.prune.createdAt = time.Time{}
	return nil
}
