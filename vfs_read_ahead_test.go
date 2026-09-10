//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

type readAheadClient struct {
	ReplicaClient
	calls      atomic.Int32
	beforeRead func()
	truncate   bool
}

func (c *readAheadClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	if c.beforeRead != nil {
		c.beforeRead()
	}
	r, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err == nil && c.truncate {
		defer r.Close()
		b, err := io.ReadAll(io.LimitReader(r, size-1))
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	return r, err
}

func TestVFSFileReadAhead(t *testing.T) {
	for _, name := range []string{"adjacent", "small-cache", "byte-limit", "newer-page", "dirty-page", "gap", "changed-index", "truncated"} {
		t.Run(name, func(t *testing.T) {
			base := newMockReplicaClient()
			base.addFixture(t, buildLTXFixtureWithPages(t, 1, DefaultPageSize, []uint32{1, 2, 3, 4, 5}, 'a'))
			client := &readAheadClient{ReplicaClient: base}
			f := NewVFSFile(client, "read-ahead.db", slog.New(slog.NewTextHandler(io.Discard, nil)))
			f.PollInterval = time.Hour
			if name == "small-cache" {
				f.CacheSize = DefaultPageSize
			}
			if err := f.Open(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.Close() })
			f.cache.Purge()
			client.calls.Store(0)
			wantCached := readAheadPages
			switch name {
			case "small-cache":
				wantCached = 1
			case "byte-limit":
				elem := f.index[2]
				elem.Size = readAheadBytes
				f.index[2] = elem
				wantCached = 1
			case "newer-page":
				elem := f.index[2]
				elem.MaxTXID++
				f.index[2] = elem
				wantCached = 1
			case "dirty-page":
				f.dirty = map[uint32]int64{2: 0}
				wantCached = 1
			case "gap":
				elem := f.index[2]
				elem.Offset++
				f.index[2] = elem
				wantCached = 1
			case "changed-index":
				client.beforeRead = func() {
					f.mu.Lock()
					elem := f.index[2]
					elem.MaxTXID++
					f.index[2] = elem
					f.mu.Unlock()
				}
				wantCached--
			case "truncated":
				client.truncate = true
				wantCached = 0
			}
			_, err := f.ReadAt(make([]byte, DefaultPageSize), 0)
			if (err != nil) != (name == "truncated") {
				t.Fatalf("read: %v", err)
			}
			if f.cache.Len() != wantCached {
				t.Fatalf("cached %d pages, want %d", f.cache.Len(), wantCached)
			}
			if name == "changed-index" && f.cache.Contains(2) {
				t.Fatal("read-ahead cached a stale page version")
			}
			if name == "adjacent" {
				for pgno := uint32(2); pgno <= readAheadPages; pgno++ {
					data := make([]byte, DefaultPageSize)
					if _, err := f.ReadAt(data, int64(pgno-1)*DefaultPageSize); err != nil {
						t.Fatal(err)
					}
					if data[0] != 'a' {
						t.Fatalf("wrong page data: %d", pgno)
					}
				}
			}
			wantRequests := int32(1)
			if name == "truncated" {
				wantRequests = pageFetchRetryAttempts
			}
			if client.calls.Load() != wantRequests {
				t.Fatalf("remote requests: %d, want %d", client.calls.Load(), wantRequests)
			}
		})
	}
}
