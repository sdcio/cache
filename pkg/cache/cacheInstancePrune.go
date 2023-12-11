package cache

import (
	"context"
	"errors"

	"github.com/rs/xid"
)

func (ci *cacheInstance) createPruneID(ctx context.Context) (string, error) {
	if ci.pruneID != "" {
		return ci.pruneID, nil
	}
	// increment current prune index
	ci.pruneIndex++
	// persist prune index
	err := ci.store.SetPruneIndex(ctx, ci.cfg.Name, ci.pruneIndex)
	if err != nil {
		return "", err
	}
	// TODO: consider other id generation methods
	// https://blog.kowalczyk.info/article/JyRZ/generating-good-unique-ids-in-go.html
	id := xid.New()
	ci.pruneID = id.String()
	return ci.pruneID, nil
}

func (ci *cacheInstance) applyPrune(ctx context.Context, id string) error {
	if ci.pruneID == "" || id != ci.pruneID {
		return errors.New("unknown prune transaction id")
	}
	err := ci.store.Prune(ctx, ci.cfg.Name, "config", ci.pruneIndex)
	if err != nil {
		return err
	}
	err = ci.store.Prune(ctx, ci.cfg.Name, "state", ci.pruneIndex)
	if err != nil {
		return err
	}
	ci.pruneID = ""
	return nil
}
