package s3fs

import (
	"container/list"
	"io"
	"io/fs"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

// entryOverhead approximates the fixed memory cost of a cache entry so
// metadata-only entries still consume budget.
const entryOverhead = 256

// cacheEntry is the immutable cached state of one object key or directory
// prefix (trailing "/"). exists reports whether the key was present; head is
// set for object entries; data holds the body when it fit the cache.
type cacheEntry struct {
	key     string
	exists  bool
	head    *s3.HeadObjectOutput
	data    []byte
	expires time.Time // zero means never

	elem *list.Element
	cost int64
}

// objCache is an LRU+TTL cache over HeadObject results, object bodies and
// directory-existence checks for one S3FS instance. Entries are immutable;
// updates replace them wholesale. Safe for concurrent use.
type objCache struct {
	maxBytes int64
	ttl      time.Duration

	mu      sync.Mutex
	entries map[string]*cacheEntry
	lru     *list.List // front = most recently used
	used    int64
}

func newObjCache(maxBytes int64, ttl time.Duration) *objCache {
	return &objCache{
		maxBytes: maxBytes,
		ttl:      ttl,
		entries:  make(map[string]*cacheEntry),
		lru:      list.New(),
	}
}

// maxDataBytes is the largest object body the cache holds, keeping one big
// object (e.g. a packfile) from evicting everything else.
func (c *objCache) maxDataBytes() int64 { return c.maxBytes / 8 }

// lookup returns the entry for key, refreshing its LRU position. Expired
// entries are dropped and reported as misses.
func (c *objCache) lookup(key string) (*cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return nil, false
	}
	if !e.validLocked() {
		c.removeLocked(e)
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	return e, true
}

func (e *cacheEntry) validLocked() bool {
	return e.expires.IsZero() || time.Now().Before(e.expires)
}

// store inserts or replaces the entry for key. data is copied; bodies larger
// than maxDataBytes are dropped, keeping metadata only.
func (c *objCache) store(key string, exists bool, head *s3.HeadObjectOutput, data []byte) {
	if data != nil {
		if int64(len(data)) > c.maxDataBytes() {
			data = nil
		} else {
			cp := make([]byte, len(data))
			copy(cp, data)
			data = cp
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(key, exists, head, data)
}

func (c *objCache) storeLocked(key string, exists bool, head *s3.HeadObjectOutput, data []byte) {
	if old := c.entries[key]; old != nil {
		c.removeLocked(old)
	}
	e := &cacheEntry{
		key:    key,
		exists: exists,
		head:   head,
		data:   data,
		cost:   entryOverhead + int64(len(key)) + int64(len(data)),
	}
	if c.ttl > 0 {
		e.expires = time.Now().Add(c.ttl)
	}
	if e.cost > c.maxBytes {
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
		c.removeLocked(back.Value.(*cacheEntry))
	}
}

func (c *objCache) removeLocked(e *cacheEntry) {
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	if e.elem != nil {
		c.lru.Remove(e.elem)
		e.elem = nil
		c.used -= e.cost
	}
}

// storeWrite records a successful PUT of key: the entry becomes the freshly
// written object and every ancestor directory is known to exist.
func (c *objCache) storeWrite(key string, data []byte, meta map[string]string) {
	var body []byte
	if int64(len(data)) <= c.maxDataBytes() {
		body = make([]byte, len(data))
		copy(body, data)
	}
	head := &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(data))),
		LastModified:  aws.Time(time.Now()),
		Metadata:      meta,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(key, true, head, body)
	c.markDirsLocked(key)
}

// storeCopied records a successful server-side copy: dst now exists, with
// the source's cached state when available.
func (c *objCache) storeCopied(srcKey, dstKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if src := c.entries[srcKey]; src != nil && src.exists && src.validLocked() && src.head != nil {
		c.storeLocked(dstKey, true, src.head, src.data)
	} else if old := c.entries[dstKey]; old != nil {
		c.removeLocked(old) // fresh state unknown; refetch on demand
	}
	c.markDirsLocked(dstKey)
}

// dropDeleted invalidates key and its ancestor directory entries after a
// delete; ancestors may have become empty.
func (c *objCache) dropDeleted(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.removeLocked(e)
	}
	for i := 0; i < len(key); i++ {
		if key[i] != '/' {
			continue
		}
		if e := c.entries[key[:i+1]]; e != nil {
			c.removeLocked(e)
		}
	}
}

// markDirsLocked records that every ancestor directory prefix of key exists.
func (c *objCache) markDirsLocked(key string) {
	for i := 0; i < len(key); i++ {
		if key[i] != '/' {
			continue
		}
		p := key[:i+1]
		if e := c.entries[p]; e != nil && e.exists && e.validLocked() {
			continue
		}
		c.storeLocked(p, true, nil, nil)
	}
}

// cachedOpen serves an O_RDONLY open from the cache, fetching bodies that
// fit the cache on miss. ok=false means the caller should stream from S3.
func (s *S3FS) cachedOpen(p string, h *s3.HeadObjectOutput) (billy.File, bool, error) {
	if s.cache == nil {
		return nil, false, nil
	}
	key := s.key(p)
	if e, ok := s.cache.lookup(key); ok && e.exists && e.data != nil {
		return newMemReadFile(p, infoFromHeadValue(path.Base(p), h), e.data), true, nil
	}
	if aws.ToInt64(h.ContentLength) > s.cache.maxDataBytes() {
		return nil, false, nil
	}
	data, err := s.getAll(key)
	if err != nil {
		if isNotFound(err) {
			return nil, false, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist}
		}
		return nil, false, err
	}
	s.cache.store(key, true, h, data)
	return newMemReadFile(p, infoFromHeadValue(path.Base(p), h), data), true, nil
}

// memReadFile is a read-only billy.File over a cached object body. data is
// shared with the cache and must not be mutated.
type memReadFile struct {
	name string
	info fileInfo
	data []byte

	mu     sync.Mutex
	pos    int64
	closed bool
}

var (
	_ billy.File   = (*memReadFile)(nil)
	_ billy.Locker = (*memReadFile)(nil)
)

func newMemReadFile(name string, info fileInfo, data []byte) *memReadFile {
	return &memReadFile{name: name, info: info, data: data}
}

func (f *memReadFile) Name() string { return f.name }

func (f *memReadFile) Stat() (fs.FileInfo, error) {
	info := f.info
	return &info, nil
}

func (f *memReadFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memReadFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrInvalid}
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memReadFile) Seek(offset int64, whence int) (int64, error) {
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
		abs = int64(len(f.data)) + offset
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	f.pos = abs
	return abs, nil
}

func (f *memReadFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
}

func (f *memReadFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: fs.ErrPermission}
}

func (f *memReadFile) Truncate(size int64) error {
	return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
}

func (f *memReadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}

// Lock implements billy.Locker as a no-op.
func (f *memReadFile) Lock() error { return nil }

// Unlock implements billy.Locker as a no-op.
func (f *memReadFile) Unlock() error { return nil }
