package s3fs

import (
	"container/list"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

// defaultSpillLimit is the write-buffer size above which contents move to a
// spill file when the disk cache is enabled and no memory cache sets the
// threshold.
const defaultSpillLimit = 8 << 20

// diskCache stores object bodies as local files, serving opens of objects
// too large for the in-memory cache. Entries are validated by ETag on every
// open and additionally expire after the configured ttl, so a stale ETag or
// an expired entry is simply a miss. The directory must be used by a single
// process; files left over from a previous run are removed lazily on first
// use.
type diskCache struct {
	dir      string
	maxBytes int64
	ttl      time.Duration

	initOnce sync.Once
	initErr  error
	seq      atomic.Int64

	mu      sync.Mutex
	entries map[string]*diskEntry // by object key
	lru     *list.List            // front = most recently used
	used    int64
}

type diskEntry struct {
	key     string
	etag    string
	size    int64
	path    string
	expires time.Time // zero means never
	elem    *list.Element
	cost    int64
}

func newDiskCache(dir string, maxBytes int64, ttl time.Duration) *diskCache {
	return &diskCache{
		dir:      dir,
		maxBytes: maxBytes,
		ttl:      ttl,
		entries:  make(map[string]*diskEntry),
		lru:      list.New(),
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
	e := c.entries[key]
	if e == nil || e.etag != etag {
		c.mu.Unlock()
		return nil, false
	}
	if !e.validLocked() {
		c.removeLocked(e)
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	p := e.path
	c.mu.Unlock()

	f, err := os.Open(p)
	if err != nil {
		c.mu.Lock()
		if c.entries[key] == e {
			c.removeLocked(e)
		}
		c.mu.Unlock()
		return nil, false
	}
	return f, true
}

func (e *diskEntry) validLocked() bool {
	return e.expires.IsZero() || time.Now().Before(e.expires)
}

// insert registers path as the cached body of key, replacing any previous
// entry and evicting least-recently-used files over budget. The cache owns
// the file afterwards.
func (c *diskCache) insert(key, etag, path string, size int64) {
	e := &diskEntry{key: key, etag: etag, size: size, path: path, cost: size + entryOverhead}
	if c.ttl > 0 {
		e.expires = time.Now().Add(c.ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.entries[key]; old != nil {
		c.removeLocked(old)
	}
	if e.cost > c.maxBytes {
		os.Remove(path)
		return
	}
	e.elem = c.lru.PushFront(e)
	c.entries[key] = e
	c.used += e.cost
	for c.used > c.maxBytes {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeLocked(back.Value.(*diskEntry))
	}
}

// removeLocked drops the entry and unlinks its file; open handles keep
// reading the unlinked inode (POSIX semantics).
func (c *diskCache) removeLocked(e *diskEntry) {
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	if e.elem != nil {
		c.lru.Remove(e.elem)
		e.elem = nil
		c.used -= e.cost
	}
	os.Remove(e.path)
}

// dropDeleted invalidates the entry for key after a delete or overwrite.
func (c *diskCache) dropDeleted(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.removeLocked(e)
	}
}

// copied mirrors a server-side copy: dst gets a hard link to src's cached
// body, so opening dst needs no download (go-git renames finished packfiles
// into place).
func (c *diskCache) copied(srcKey, dstKey string) {
	c.mu.Lock()
	src := c.entries[srcKey]
	if src == nil {
		if old := c.entries[dstKey]; old != nil {
			c.removeLocked(old)
		}
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

// diskFetch streams the object into the disk cache and returns a handle
// serving it. ok=false means the caller should stream from S3 instead.
func (s *S3FS) diskFetch(key string, h *s3.HeadObjectOutput, p string) (billy.File, bool, error) {
	c := s.disk
	if c.ensure() != nil {
		return nil, false, nil
	}
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, false, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist}
		}
		return nil, false, err
	}
	defer out.Body.Close()

	tmp := c.newPath("c")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, nil // disk unusable; stream instead
	}
	size, err := io.Copy(f, out.Body)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, false, err
	}
	info := infoFromHeadValue(path.Base(p), h)
	info.size = size
	etag := aws.ToString(out.ETag)
	if etag == "" {
		etag = aws.ToString(h.ETag)
	}
	if etag == "" {
		// cannot validate later; serve this handle but do not cache
		os.Remove(tmp)
		return newDiskReadFile(f, p, info), true, nil
	}
	c.insert(key, etag, tmp, size)
	return newDiskReadFile(f, p, info), true, nil
}

// diskReadFile is a read-only billy.File over a locally cached object body.
// All reads use pread, so ReadAt is safe for concurrent use.
type diskReadFile struct {
	f    *os.File
	name string
	info fileInfo

	mu     sync.Mutex
	pos    int64
	closed bool
}

var (
	_ billy.File   = (*diskReadFile)(nil)
	_ billy.Locker = (*diskReadFile)(nil)
)

func newDiskReadFile(f *os.File, name string, info fileInfo) *diskReadFile {
	return &diskReadFile{f: f, name: name, info: info}
}

func (f *diskReadFile) Name() string { return f.name }

func (f *diskReadFile) Stat() (fs.FileInfo, error) {
	info := f.info
	return &info, nil
}

func (f *diskReadFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if f.pos >= f.info.size {
		return 0, io.EOF
	}
	n, err := f.f.ReadAt(p, f.pos)
	f.pos += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (f *diskReadFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrInvalid}
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= f.info.size {
		return 0, io.EOF
	}
	return f.f.ReadAt(p, off)
}

func (f *diskReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrClosed}
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		abs = f.info.size + offset
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	f.pos = abs
	return abs, nil
}

func (f *diskReadFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
}

func (f *diskReadFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: fs.ErrPermission}
}

func (f *diskReadFile) Truncate(size int64) error {
	return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
}

func (f *diskReadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return f.f.Close()
}

// Lock implements billy.Locker as a no-op.
func (f *diskReadFile) Lock() error { return nil }

// Unlock implements billy.Locker as a no-op.
func (f *diskReadFile) Unlock() error { return nil }

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
