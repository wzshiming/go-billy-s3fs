package s3fs

import (
	"io"
	"sync"
)

// This file holds the sync.Pools recycling the short-lived allocations on
// hot paths: stream-copy chunks, write-buffer backing arrays and LRU index
// entries.

// copyBufSize is the chunk size used by pooled stream copies.
const copyBufSize = 32 << 10

// maxPooledDataBytes caps the capacity of recycled write buffers so a few
// huge uploads do not pin memory in the pool.
const maxPooledDataBytes = 4 << 20

// copyBufPool recycles the fixed-size chunks used by copyStream. Slices
// are stored via pointer so Put does not allocate a slice header.
var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufSize)
		return &b
	},
}

// writerOnly hides a ReadFrom method on dst so io.CopyBuffer cannot bypass
// the pooled buffer (*os.File implements io.ReaderFrom).
type writerOnly struct{ io.Writer }

// copyStream is io.Copy with a pooled chunk buffer.
func copyStream(dst io.Writer, src io.Reader) (int64, error) {
	bp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bp)
	return io.CopyBuffer(writerOnly{dst}, src, *bp)
}

// dataPool recycles write-buffer backing arrays (memBuf.data).
var dataPool = sync.Pool{New: func() any { return new([]byte) }}

// pooledData returns a zero-length slice backed by recycled capacity; the
// capacity may be zero when the pool is empty. Contents are not zeroed;
// memBuf.grow clears reused capacity before exposing it.
func pooledData() []byte {
	return (*dataPool.Get().(*[]byte))[:0]
}

// recycleData returns a backing array to the pool. The caller must own the
// only remaining reference. Oversized arrays are left to the GC.
func recycleData(b []byte) {
	if cap(b) == 0 || cap(b) > maxPooledDataBytes {
		return
	}
	b = b[:0]
	dataPool.Put(&b)
}
