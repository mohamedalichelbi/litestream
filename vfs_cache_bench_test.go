//go:build vfs

package litestream

import (
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"testing"
	"time"
)

func TestVFSFileCachePageAllocation(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))
	f := NewVFSFile(client, "cache.db", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.ReadAt(make([]byte, DefaultPageSize), 0); err != nil {
		t.Fatal(err)
	}
	page, ok := f.cache.Get(1)
	if !ok || len(page) != DefaultPageSize || cap(page) != DefaultPageSize {
		t.Fatalf("cached page: found=%v length=%d capacity=%d, want %d bytes", ok, len(page), cap(page), DefaultPageSize)
	}
}

func BenchmarkVFSFileIdleDatabases(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			client := newMockReplicaClient()
			client.addFixture(b, buildLTXFixture(b, 1, 'a'))
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			for iteration := 0; iteration < b.N; iteration++ {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				files := make([]*VFSFile, 0, count)
				for i := 0; i < count; i++ {
					f := NewVFSFile(client, fmt.Sprintf("idle-%d.db", i), logger)
					f.PollInterval = time.Hour
					if err := f.Open(); err != nil {
						b.Fatal(err)
					}
					files = append(files, f)
					if _, err := f.ReadAt(make([]byte, DefaultPageSize), 0); err != nil {
						b.Fatal(err)
					}
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(count), "heap-bytes/database")
				for _, f := range files {
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
