package s3fs

import (
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

// entryOverhead approximates the fixed memory cost of a cache entry so
// metadata-only entries still consume budget.
const entryOverhead = 256

// lruEntry pairs a cached value with its bookkeeping.
type lruEntry[V any] struct {
	val     V
	cost    int64
	expires time.Time // zero means never
}

// lruIndex layers a byte budget, TTL and type safety over groupcache's
// entry-count LRU. It is not safe for concurrent use; callers provide
// locking. onEvict, when set, runs for every value that leaves the index,
// including values rejected by put for being over budget.
type lruIndex[V any] struct {
	maxBytes int64
	ttl      time.Duration
	onEvict  func(V)

	cache *lru.Cache
	used  int64
	// entryPool recycles lruEntry shells between put and eviction; the
	// cached values themselves are never pooled.
	entryPool sync.Pool
}

func newLRUIndex[V any](maxBytes int64, ttl time.Duration, onEvict func(V)) *lruIndex[V] {
	x := &lruIndex[V]{
		maxBytes: maxBytes,
		ttl:      ttl,
		onEvict:  onEvict,
	}
	// MaxEntries stays 0 (unlimited); eviction is driven by the byte
	// budget in put.
	x.cache = &lru.Cache{
		OnEvicted: func(_ lru.Key, value any) {
			e := value.(*lruEntry[V])
			x.used -= e.cost
			if x.onEvict != nil {
				x.onEvict(e.val)
			}
			var zero V
			e.val = zero // do not retain the evicted value through the pool
			x.entryPool.Put(e)
		},
	}
	return x
}

// get returns the value for key, refreshing its LRU position. Expired
// entries are dropped and reported as misses.
func (x *lruIndex[V]) get(key string) (V, bool) {
	var zero V
	v, ok := x.cache.Get(key)
	if !ok {
		return zero, false
	}
	e := v.(*lruEntry[V])
	if !e.expires.IsZero() && !time.Now().Before(e.expires) {
		x.cache.Remove(key)
		return zero, false
	}
	return e.val, true
}

// put inserts or replaces the entry for key, evicting least-recently-used
// entries once over budget. Values costing more than the whole budget are
// rejected.
func (x *lruIndex[V]) put(key string, v V, cost int64) {
	// Add replaces silently; Remove first so the old value is released
	// through OnEvicted.
	x.cache.Remove(key)
	if cost > x.maxBytes {
		if x.onEvict != nil {
			x.onEvict(v)
		}
		return
	}
	e, _ := x.entryPool.Get().(*lruEntry[V])
	if e == nil {
		e = new(lruEntry[V])
	}
	e.val, e.cost = v, cost
	e.expires = time.Time{}
	if x.ttl > 0 {
		e.expires = time.Now().Add(x.ttl)
	}
	x.cache.Add(key, e)
	x.used += cost
	for x.used > x.maxBytes && x.cache.Len() > 0 {
		x.cache.RemoveOldest()
	}
}

// delete removes key when present.
func (x *lruIndex[V]) delete(key string) {
	x.cache.Remove(key)
}
