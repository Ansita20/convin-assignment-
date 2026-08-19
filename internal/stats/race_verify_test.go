package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordConcurrentNoLostUpdates(t *testing.T) {
	c := stats.NewCache()

	const goroutines = 50
	const perGoroutine = 20
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Record("shared_acc", 1)
			}
		}()
	}
	wg.Wait()

	got := c.Get("shared_acc")
	want := int64(goroutines * perGoroutine)
	if got.CallCount != want {
		t.Fatalf("got CallCount=%d, want %d (lost updates)", got.CallCount, want)
	}
	if got.TotalDurationSec != want {
		t.Fatalf("got TotalDurationSec=%d, want %d (lost updates)", got.TotalDurationSec, want)
	}
}

func TestCacheRecordConcurrentNewAccounts(t *testing.T) {
	c := stats.NewCache()

	const goroutines = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Record("brand_new_acc", 1)
		}()
	}
	wg.Wait()

	got := c.Get("brand_new_acc")
	if got.CallCount != goroutines {
		t.Fatalf("got CallCount=%d, want %d", got.CallCount, goroutines)
	}
}
