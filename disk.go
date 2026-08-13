package s3fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// defaultSpillLimit is the write-buffer size above which contents move to a
// spill file when the disk cache is enabled and no memory cache sets the
// threshold.
const defaultSpillLimit = 8 << 20

// diskEntry locates the cached body of one object key on disk.
type diskEntry struct {
	etag string
	size int64
	path string
}

// diskCache stores object bodies as local files, serving opens of objects
// too large for the in-memory cache. Entries are validated by ETag on every
// open and additionally expire after the configured ttl, so a stale ETag or
// an expired entry is simply a miss. The directory must be used by a single
// process; files left over from a previous run are removed lazily on first
// use.
type diskCache struct {
	dir      string
	maxBytes int64

	initOnce sync.Once
	initErr  error
	seq      atomic.Int64

	mu  sync.Mutex
	idx *lruIndex[*diskEntry]
}

func newDiskCache(dir string, maxBytes int64, ttl time.Duration) *diskCache {
	return &diskCache{
		dir:      dir,
		maxBytes: maxBytes,
		idx: newLRUIndex(maxBytes, ttl, func(e *diskEntry) {
			// open handles keep reading the unlinked inode (POSIX semantics)
			os.Remove(e.path)
		}),
	}
}

// ensure creates the cache directory and sweeps leftovers from a previous
// process. An error disables the disk tier for the operation at hand.
func (c *diskCache) ensure() error {
	c.initOnce.Do(func() {
		if err := os.MkdirAll(c.dir, 0o755); err != nil {
			c.initErr = err
			return
		}
		leftovers, _ := filepath.Glob(filepath.Join(c.dir, "s3fs-*"))
		for _, p := range leftovers {
			os.Remove(p)
		}
	})
	return c.initErr
}

// newPath returns a fresh unique file path; kind is "c" for cache entries
// and "w" for write spill buffers.
func (c *diskCache) newPath(kind string) string {
	return filepath.Join(c.dir, fmt.Sprintf("s3fs-%s-%d", kind, c.seq.Add(1)))
}

// open returns a fresh handle over the cached body of key when the cached
// ETag matches and the entry has not expired. A vanished file drops the
// entry and reports a miss.
func (c *diskCache) open(key, etag string) (*os.File, bool) {
	if etag == "" {
		return nil, false
	}
	c.mu.Lock()
	e, ok := c.idx.get(key)
	if !ok || e.etag != etag {
		c.mu.Unlock()
		return nil, false
	}
	p := e.path
	c.mu.Unlock()

	f, err := os.Open(p)
	if err != nil {
		c.mu.Lock()
		if cur, ok := c.idx.get(key); ok && cur == e {
			c.idx.delete(key)
		}
		c.mu.Unlock()
		return nil, false
	}
	return f, true
}

// has reports whether the cache holds a not-yet-expired body for key; the
// ETag is not validated.
func (c *diskCache) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.idx.get(key)
	return ok
}

// insert registers path as the cached body of key, replacing any previous
// entry and evicting least-recently-used files over budget. The cache owns
// the file afterwards.
func (c *diskCache) insert(key, etag, path string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idx.put(key, &diskEntry{etag: etag, size: size, path: path}, size+entryOverhead)
}

// dropDeleted invalidates the entry for key after a delete or overwrite.
func (c *diskCache) dropDeleted(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idx.delete(key)
}

// copied mirrors a server-side copy: dst gets a hard link to src's cached
// body, so opening dst needs no download (go-git renames finished packfiles
// into place).
func (c *diskCache) copied(srcKey, dstKey string) {
	c.mu.Lock()
	src, ok := c.idx.get(srcKey)
	if !ok {
		c.idx.delete(dstKey)
		c.mu.Unlock()
		return
	}
	etag, size, srcPath := src.etag, src.size, src.path
	c.mu.Unlock()

	dstPath := c.newPath("c")
	if err := os.Link(srcPath, dstPath); err != nil {
		c.dropDeleted(dstKey)
		return
	}
	c.insert(dstKey, etag, dstPath, size)
}

// adopt hard-links an uploaded spill file into the cache under key, saving
// the re-download on the next open.
func (c *diskCache) adopt(key, etag, srcPath string, size int64) {
	if etag == "" || c.ensure() != nil {
		return
	}
	dstPath := c.newPath("c")
	if err := os.Link(srcPath, dstPath); err != nil {
		return
	}
	c.insert(key, etag, dstPath, size)
}

// fileBuf is a writeFile buffer spilled to a local file. All access is
// offset-based, so uploads reading the same fd do not disturb it.
type fileBuf struct {
	f    *os.File
	path string
	sz   int64
}

// newSpill creates an empty write spill file; the caller owns it and must
// call destroy.
func (c *diskCache) newSpill() (*fileBuf, error) {
	if err := c.ensure(); err != nil {
		return nil, err
	}
	p := c.newPath("w")
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileBuf{f: f, path: p}, nil
}

func (b *fileBuf) len() int64 { return b.sz }

func (b *fileBuf) readAt(p []byte, off int64) (int, error) {
	if off >= b.sz {
		return 0, io.EOF
	}
	return b.f.ReadAt(p, off)
}

func (b *fileBuf) writeAt(p []byte, off int64) (int, error) {
	n, err := b.f.WriteAt(p, off)
	if end := off + int64(n); end > b.sz {
		b.sz = end
	}
	return n, err
}

func (b *fileBuf) truncate(size int64) error {
	if err := b.f.Truncate(size); err != nil {
		return err
	}
	b.sz = size
	return nil
}

func (b *fileBuf) bytes() ([]byte, bool) { return nil, false }

func (b *fileBuf) snapshot() ([]byte, error) {
	if b.sz == 0 {
		return nil, nil
	}
	data := make([]byte, b.sz)
	if _, err := b.f.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}

func (b *fileBuf) destroy() {
	b.f.Close()
	os.Remove(b.path)
}
