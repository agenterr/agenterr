package template

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestExtractConcurrent(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				body := fmt.Sprintf("worker %d iteration %d done", g, i)
				id, vars, ok, err := e.Extract(context.Background(), int64(g%3+1), body)
				if err != nil || !ok {
					t.Errorf("extract: ok=%v err=%v", ok, err)
					return
				}
				if got, ok2 := e.Reconstruct(int64(g%3+1), id, vars); !ok2 || got != body {
					t.Errorf("round trip: %q", got)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
