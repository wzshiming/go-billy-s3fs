package s3fs_test

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
)

// lockTimeout bounds waits for events that must happen.
const lockTimeout = 5 * time.Second

func mustLock(t *testing.T, f billy.File) {
	t.Helper()
	if err := f.(billy.Locker).Lock(); err != nil {
		t.Fatalf("Lock %s: %v", f.Name(), err)
	}
}

func mustUnlock(t *testing.T, f billy.File) {
	t.Helper()
	if err := f.(billy.Locker).Unlock(); err != nil {
		t.Fatalf("Unlock %s: %v", f.Name(), err)
	}
}

// lockAsync locks f in a goroutine and reports acquisition on the returned
// channel.
func lockAsync(t *testing.T, f billy.File) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- f.(billy.Locker).Lock() }()
	return ch
}

func assertBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("Lock returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertAcquired(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("Lock: %v", err)
		}
	case <-time.After(lockTimeout):
		t.Fatal("Lock still blocked")
	}
}

func TestLockCapability(t *testing.T) {
	bfs := newTestFS(t)
	if !billy.CapabilityCheck(bfs, billy.LockCapability) {
		t.Fatal("LockCapability not advertised")
	}
}

func TestLockReentrantAndUnlockIdempotent(t *testing.T) {
	bfs := newTestFS(t)
	f, err := bfs.Create("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// unlock without lock is a no-op, like flock
	mustUnlock(t, f)
	mustLock(t, f)
	// relock by the same handle is a no-op, like flock
	mustLock(t, f)
	mustUnlock(t, f)
	mustUnlock(t, f)
}

func TestLockContention(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "/a.txt", "content")

	a, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	mustLock(t, a)
	ch := lockAsync(t, b)
	assertBlocked(t, ch)
	mustUnlock(t, a)
	assertAcquired(t, ch)
	mustUnlock(t, b)
}

func TestLockReleasedOnClose(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "/a.txt", "content")

	a, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	mustLock(t, a)

	b, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ch := lockAsync(t, b)
	assertBlocked(t, ch)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	assertAcquired(t, ch)
	mustUnlock(t, b)
}

// TestLockAcrossHandleTypes locks via a writer and contends with a shared
// reader on the live write, then with a plain reader after upload.
func TestLockAcrossHandleTypes(t *testing.T) {
	bfs := newTestFS(t)

	w, err := bfs.Create("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("content")); err != nil {
		t.Fatal(err)
	}
	// O_RDONLY open of a live write returns a shared read handle
	shared, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}

	mustLock(t, w)
	ch := lockAsync(t, shared)
	assertBlocked(t, ch)
	mustUnlock(t, w)
	assertAcquired(t, ch)
	mustUnlock(t, shared)
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}

	mustLock(t, w)
	if err := w.Close(); err != nil { // upload and release
		t.Fatal(err)
	}

	r, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mustLock(t, r)
	mustUnlock(t, r)
}

func TestLockClosedHandle(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "/a.txt", "content")

	f, err := bfs.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.(billy.Locker).Lock(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Lock on closed handle: got %v, want ErrClosed", err)
	}
	if err := f.(billy.Locker).Unlock(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Unlock on closed handle: got %v, want ErrClosed", err)
	}
}

// TestLockVariants runs the contention flow against every cache variant,
// since each returns different read handle implementations.
func TestLockVariants(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) {
			bfs := newTestFS(t, v.opts(t)...)
			writeFull(t, bfs, "/a.txt", "content")

			a, err := bfs.Open("/a.txt")
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := bfs.OpenFile("/a.txt", os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()

			mustLock(t, a)
			ch := lockAsync(t, b)
			assertBlocked(t, ch)
			mustUnlock(t, a)
			assertAcquired(t, ch)
			mustUnlock(t, b)
		})
	}
}
