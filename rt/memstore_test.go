package rt_test

import (
	"testing"

	"github.com/abmax777/ratchet/rt"
	"github.com/abmax777/ratchet/rt/storetest"
)

func TestMemStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) rt.Store { return rt.NewMemStore() })
}
