package filesystem_test

import (
	"testing"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/store/filesystem"
)

func TestStaleHandleCannotTouchRecreatedStore(t *testing.T) {
	for name, closeFn := range map[string]func(store.Store) error{
		"Delete": store.Store.Delete,
		"Close":  store.Store.Close,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			stale, err := filesystem.NewFilesystemStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := closeFn(stale); err != nil {
				t.Fatal(err)
			}
			fresh, err := filesystem.NewFilesystemStore(dir)
			if err != nil {
				t.Fatal(err)
			}

			if err := stale.IntentModify(ctx, "stale", []byte("data")); err == nil {
				t.Errorf("IntentModify after %s succeeded, want an error", name)
			}
			names, err := fresh.IntentsList(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(names) != 0 {
				t.Errorf("store re-created at the same path has intents %v, want none", names)
			}
		})
	}
}
