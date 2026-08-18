package enginestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// benchStore builds a store with ~200k templated rows across 10
// services in one compacted segment — a miniature of the real bench
// corpus.
func benchStore(b *testing.B) (*Store, int64) {
	b.Helper()
	s := openStore(b, b.TempDir(), Options{})
	ctx := context.Background()
	p, err := s.CreateProject(ctx, "bench", 30)
	if err != nil {
		b.Fatal(err)
	}
	base := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	const n = 200_000
	batch := make([]store.Entry, 0, 1000)
	for i := 0; i < n; i++ {
		svc := fmt.Sprintf("svc%d", i%10)
		body := fmt.Sprintf("request handled path=/api/v1/items/%d status=200 dur=%dms", i, i%97)
		// 997 is coprime with the 10-service cycle, so these rare markers
		// land on every service across the run (unlike i%1000==999, which
		// would always coincide with svc9 and starve BenchmarkSearchScoped
		// of hits on svc3).
		if i%997 == 996 {
			body = fmt.Sprintf("record not found for id %d", i)
		}
		batch = append(batch, store.Entry{Log: core.Log{
			ProjectID: p.ID, Time: base.Add(time.Duration(i) * 400 * time.Millisecond),
			Severity: core.SeverityError, Service: svc, Body: body,
		}})
		if len(batch) == 1000 {
			if _, err := s.WriteBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := s.FlushAll(); err != nil {
		b.Fatal(err)
	}
	if err := s.CompactAll(ctx); err != nil {
		b.Fatal(err)
	}
	return s, p.ID
}

func BenchmarkSearchScoped(b *testing.B) {
	s, pid := benchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Service: "svc3", Query: "record not found"})
		if err != nil {
			b.Fatal(err)
		}
		if len(logs) == 0 {
			b.Fatal("expected hits")
		}
	}
}

func BenchmarkSearchUnscopedNoHit(b *testing.B) {
	s, pid := benchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "zzz never present"})
		if err != nil {
			b.Fatal(err)
		}
		if len(logs) != 0 {
			b.Fatal("expected no hits")
		}
	}
}
