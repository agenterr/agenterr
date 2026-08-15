package enginestore_test

import (
	"path/filepath"
	"testing"

	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/enginestore"
	"github.com/agenterr/agenterr/internal/store/storetest"
)

func TestEngineStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := enginestore.Open(filepath.Join(t.TempDir(), "agenterr.db"), enginestore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
