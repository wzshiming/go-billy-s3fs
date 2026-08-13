package s3fs

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// memEntry is the immutable cached state of one object key or directory
// prefix (trailing "/"). exists reports whether the key was present; head is
// set for object entries; data holds the body when it fit the cache.
type memEntry struct {
	exists bool
	head   *s3.HeadObjectOutput
	data   []byte
}

// memCache is an LRU+TTL cache over HeadObject results, object bodies and
// directory-existence checks for one S3FS instance. Entries are immutable;
// updates replace them wholesale. Safe for concurrent use.
type memCache struct {
	maxBytes int64

	mu  sync.Mutex
	idx *lruIndex[*memEntry]
}

func newMemCache(maxBytes int64, ttl time.Duration) *memCache {
	return &memCache{
		maxBytes: maxBytes,
		idx:      newLRUIndex[*memEntry](maxBytes, ttl, nil),
	}
}

// maxDataBytes is the largest object body the cache holds, keeping one big
// object (e.g. a packfile) from evicting everything else.
func (c *memCache) maxDataBytes() int64 { return c.maxBytes / 8 }

// lookup returns the entry for key, refreshing its LRU position. Expired
// entries are dropped and reported as misses.
func (c *memCache) lookup(key string) (*memEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.idx.get(key)
}

// store inserts or replaces the entry for key. data is copied; bodies larger
// than maxDataBytes are dropped, keeping metadata only.
func (c *memCache) store(key string, exists bool, head *s3.HeadObjectOutput, data []byte) {
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
	c.storeLocked(key, &memEntry{exists: exists, head: head, data: data})
}

func (c *memCache) storeLocked(key string, e *memEntry) {
	c.idx.put(key, e, entryOverhead+int64(len(key))+int64(len(e.data)))
}

// storeWrite records a successful PUT of key: the entry becomes the freshly
// written object and every ancestor directory is known to exist. data may
// be nil when the body is not to be cached (e.g. it lives in a spill file).
func (c *memCache) storeWrite(key string, size int64, etag string, data []byte, meta map[string]string) {
	var body []byte
	if data != nil && int64(len(data)) <= c.maxDataBytes() {
		body = make([]byte, len(data))
		copy(body, data)
	}
	head := &s3.HeadObjectOutput{
		ContentLength: aws.Int64(size),
		LastModified:  aws.Time(time.Now()),
		Metadata:      meta,
	}
	if etag != "" {
		head.ETag = aws.String(etag)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(key, &memEntry{exists: true, head: head, data: body})
	c.markDirsLocked(key)
}

// storeCopied records a successful server-side copy: dst now exists, with
// the source's cached state when available.
func (c *memCache) storeCopied(srcKey, dstKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if src, ok := c.idx.get(srcKey); ok && src.exists && src.head != nil {
		c.storeLocked(dstKey, &memEntry{exists: true, head: src.head, data: src.data})
	} else {
		c.idx.delete(dstKey) // fresh state unknown; refetch on demand
	}
	c.markDirsLocked(dstKey)
}

// dropDeleted invalidates key and its ancestor directory entries after a
// delete; ancestors may have become empty.
func (c *memCache) dropDeleted(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idx.delete(key)
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			c.idx.delete(key[:i+1])
		}
	}
}

// markDirsLocked records that every ancestor directory prefix of key exists.
func (c *memCache) markDirsLocked(key string) {
	for i := 0; i < len(key); i++ {
		if key[i] != '/' {
			continue
		}
		p := key[:i+1]
		if e, ok := c.idx.get(p); ok && e.exists {
			continue
		}
		c.storeLocked(p, &memEntry{exists: true})
	}
}
