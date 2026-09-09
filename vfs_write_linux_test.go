//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
)

func TestVFSFile_PartialBufferWriteFailure(t *testing.T) {
	const childEnv = "LITESTREAM_TEST_SHORT_WRITE_CHILD"
	const partialBytes = 128
	mode := os.Getenv(childEnv)
	if mode == "" {
		for _, mode := range []string{"sync", "close"} {
			t.Run(mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVFSFile_PartialBufferWriteFailure$", "-test.v")
				cmd.Env = append(os.Environ(), childEnv+"="+mode)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("short-write child: %v\n%s", err, out)
				}
			})
		}
		return
	}
	if mode != "sync" && mode != "close" {
		t.Fatalf("invalid short-write child mode: %s", mode)
	}

	base := newWriteTestReplicaClient()
	original := bytes.Repeat([]byte{'a'}, DefaultPageSize)
	pending := bytes.Repeat([]byte{'b'}, DefaultPageSize)
	replacement := bytes.Repeat([]byte{'c'}, DefaultPageSize)
	createTestLTXFile(t, base, 1, DefaultPageSize, 2, map[uint32][]byte{1: original, 2: original})
	v := NewVFS(base, slog.Default())
	v.WriteEnabled = true
	v.PollInterval = time.Hour
	v.WriteSyncInterval = time.Hour
	file, _, err := v.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	f := file.(*VFSFile)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = f.Close()
		}
	})
	if _, err := f.WriteAt(pending, DefaultPageSize); err != nil {
		t.Fatal(err)
	}

	var previous syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &previous); err != nil {
		t.Fatal(err)
	}
	signal.Ignore(syscall.SIGXFSZ)
	defer signal.Reset(syscall.SIGXFSZ)
	limit := previous
	limit.Cur = partialBytes
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	n, writeErr := f.WriteAt(replacement, DefaultPageSize)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &previous); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(writeErr, syscall.EFBIG) || n != 0 {
		t.Fatalf("expected a rejected partial buffer write: n=%d err=%v", n, writeErr)
	}
	actual := make([]byte, DefaultPageSize)
	if _, err := f.bufferFile.ReadAt(actual, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual[:partialBytes], replacement[:partialBytes]) ||
		!bytes.Equal(actual[partialBytes:], pending[partialBytes:]) {
		t.Fatal("fault did not create the expected mixed buffer page")
	}
	if _, err := f.ReadAt(actual, DefaultPageSize); err == nil {
		t.Error("read accepted a partially overwritten buffer")
	}
	if err := f.Lock(sqlite3vfs.LockShared); err == nil {
		t.Error("lock accepted a failed buffer")
	}
	if _, err := f.WriteAt(pending, DefaultPageSize); err == nil {
		t.Error("a later write reused a failed buffer")
	}
	if err := f.Truncate(0); err == nil {
		t.Error("truncate cleared failed buffer state")
	}
	if err := f.SetWriteEnabled(false); err == nil {
		t.Error("write disable accepted a failed buffer")
	}
	if _, err := f.FileControl(14, "litestream_durability_status", nil); err == nil {
		t.Error("durability status accepted a failed buffer")
	}

	if mode == "sync" {
		if err := f.Sync(0); err == nil {
			t.Error("sync accepted a partially overwritten buffer")
		}
	}
	closed = true
	if err := f.Close(); err == nil {
		t.Error("close accepted a partially overwritten buffer")
	}
	fresh := NewVFSFile(base, "recovered.db", slog.Default())
	if err := fresh.Open(); err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if fresh.Pos().TXID != 1 {
		t.Errorf("failed write published TXID %s", fresh.Pos().TXID)
	}
	if _, err := fresh.ReadAt(actual, DefaultPageSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, original) {
		t.Error("recovery lost the last synchronized page")
	}
}
