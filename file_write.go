package s3fs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
)

var (
	_ billy.File   = (*writeFile)(nil)
	_ billy.Locker = (*writeFile)(nil)
	_ billy.Syncer = (*writeFile)(nil)
)

// writeBuf is the backing store of a writeFile: in memory, or spilled to a
// local file when the disk cache is enabled and contents grow large.
type writeBuf interface {
	len() int64
	// readAt reports io.EOF at or past the end and on short reads.
	readAt(p []byte, off int64) (int, error)
	// writeAt grows the buffer with zeros when writing past the end.
	writeAt(p []byte, off int64) (int, error)
	truncate(size int64) error
	// bytes exposes the raw buffer when memory-backed.
	bytes() ([]byte, bool)
	// snapshot returns an independent copy of the contents.
	snapshot() ([]byte, error)
	// destroy releases resources once no handle can read the buffer again.
	destroy()
}

type memBuf struct{ data []byte }

// newMemBuf wraps data, drawing recycled capacity from the pool when the
// buffer starts empty. Non-nil data must be exclusively owned by the buffer:
// its backing array is recycled once grown, spilled or destroyed.
func newMemBuf(data []byte) *memBuf {
	if data == nil {
		return &memBuf{data: pooledData()}
	}
	return &memBuf{data: data}
}

func (b *memBuf) len() int64 { return int64(len(b.data)) }

func (b *memBuf) readAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *memBuf) writeAt(p []byte, off int64) (int, error) {
	b.grow(off + int64(len(p)))
	return copy(b.data[off:], p), nil
}

// grow extends the buffer to end with zeros. Capacity doubles so chunked
// appends stay O(n) overall instead of reallocating per write.
func (b *memBuf) grow(end int64) {
	if end <= int64(len(b.data)) {
		return
	}
	if end <= int64(cap(b.data)) {
		// reused capacity may hold stale bytes from an earlier truncate
		clear(b.data[len(b.data):end])
		b.data = b.data[:end]
		return
	}
	newCap := max(2*cap(b.data), int(end))
	grown := make([]byte, end, newCap)
	copy(grown, b.data)
	recycleData(b.data)
	b.data = grown
}

func (b *memBuf) truncate(size int64) error {
	if size <= int64(len(b.data)) {
		b.data = b.data[:size]
	} else {
		b.grow(size)
	}
	return nil
}

func (b *memBuf) bytes() ([]byte, bool) { return b.data, true }

func (b *memBuf) snapshot() ([]byte, error) { return append([]byte(nil), b.data...), nil }

func (b *memBuf) destroy() {
	recycleData(b.data)
	b.data = nil
}

// writeFile buffers contents in memory (or a local spill file, see
// writeBuf) and uploads to S3 on Close or Sync.
type writeFile struct {
	fs   *S3FS
	name string
	flag int
	perm fs.FileMode

	mu      sync.Mutex
	buf     writeBuf
	pos     int64
	dirty   bool
	closed  bool
	shared  int // open sharedReadFile handles
	modTime time.Time
	// spillETag is the ETag of the last spill upload, for adopting the
	// spill file into the disk cache on Close.
	spillETag string
}

func newWriteFile(s *S3FS, p string, flag int, perm fs.FileMode, data []byte, dirty bool) *writeFile {
	return newWriteFileBuf(s, p, flag, perm, newMemBuf(data), dirty)
}

func newWriteFileBuf(s *S3FS, p string, flag int, perm fs.FileMode, buf writeBuf, dirty bool) *writeFile {
	if perm == 0 {
		perm = defaultFileMode
	}
	f := &writeFile{
		fs:      s,
		name:    p,
		flag:    flag,
		perm:    perm.Perm(),
		buf:     buf,
		dirty:   dirty,
		modTime: time.Now(),
	}
	f.maybeSpillLocked(buf.len())
	s.trackWrite(f)
	return f
}

// maybeSpillLocked migrates an in-memory buffer to a spill file once it
// outgrows the threshold; failures keep the buffer in memory. Caller must
// hold f.mu (or hold the only reference).
func (f *writeFile) maybeSpillLocked(newEnd int64) {
	if f.fs.disk == nil || newEnd <= f.fs.spillThreshold() {
		return
	}
	mb, ok := f.buf.(*memBuf)
	if !ok {
		return
	}
	fb, err := f.fs.disk.newSpill()
	if err != nil {
		return
	}
	if len(mb.data) > 0 {
		if _, err := fb.writeAt(mb.data, 0); err != nil {
			fb.destroy()
			return
		}
	}
	recycleData(mb.data)
	f.buf = fb
}

// reopenState returns the mode and, when withData, a copy of the contents
// for reopening this path through a new handle. ok=false reports that the
// writer already closed, making the uploaded object authoritative.
func (f *writeFile) reopenState(withData bool) (perm fs.FileMode, data []byte, ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, nil, false, nil
	}
	if withData {
		if data, err = f.buf.snapshot(); err != nil {
			return 0, nil, false, err
		}
	}
	return f.perm, data, true, nil
}

// statIfOpen is Stat unless the writer already closed, when the uploaded
// object is authoritative.
func (f *writeFile) statIfOpen() (fs.FileInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, false
	}
	return f.statLocked(), true
}

func (f *writeFile) Name() string { return f.name }

func (f *writeFile) readable() bool {
	return f.flag&(os.O_WRONLY|os.O_RDWR) != os.O_WRONLY
}

func (f *writeFile) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statLocked(), nil
}

// statLocked builds the FileInfo of the live buffer. Caller must hold f.mu.
func (f *writeFile) statLocked() fs.FileInfo {
	return &fileInfo{
		name:    path.Base(f.name),
		size:    f.buf.len(),
		mode:    f.perm,
		modTime: f.modTime,
	}
}

func (f *writeFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if !f.readable() {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrPermission}
	}
	if f.pos >= f.buf.len() {
		return 0, io.EOF
	}
	n, err := f.buf.readAt(p, f.pos)
	f.pos += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (f *writeFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if !f.readable() {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrPermission}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrInvalid}
	}
	if off >= f.buf.len() {
		return 0, io.EOF
	}
	return f.buf.readAt(p, off)
}

func (f *writeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrClosed}
	}
	if f.flag&os.O_APPEND != 0 {
		f.pos = f.buf.len()
	}
	n, err := f.writeAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *writeFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrClosed}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: fs.ErrInvalid}
	}
	return f.writeAt(p, off)
}

// writeAt writes p at off, growing the buffer with zeros when needed.
// Caller must hold f.mu.
func (f *writeFile) writeAt(p []byte, off int64) (int, error) {
	f.maybeSpillLocked(off + int64(len(p)))
	n, err := f.buf.writeAt(p, off)
	if err != nil {
		return n, &fs.PathError{Op: "write", Path: f.name, Err: err}
	}
	f.dirty = true
	f.modTime = time.Now()
	return n, nil
}

func (f *writeFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrClosed}
	}
	abs, err := seekPos(f.pos, f.buf.len(), offset, whence)
	if err != nil {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: err}
	}
	f.pos = abs
	return abs, nil
}

func (f *writeFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrClosed}
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	}
	f.maybeSpillLocked(size)
	if err := f.buf.truncate(size); err != nil {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: err}
	}
	f.dirty = true
	f.modTime = time.Now()
	return nil
}

// flush uploads the buffer. Caller must hold f.mu.
func (f *writeFile) flush() error {
	if !f.dirty {
		return nil
	}
	meta := map[string]string{modeMetaKey: fmt.Sprintf("%o", f.perm)}
	if data, ok := f.buf.bytes(); ok {
		if err := f.fs.put(f.fs.key(f.name), data, meta); err != nil {
			return err
		}
	} else {
		etag, err := f.fs.putSpill(f.fs.key(f.name), f.buf.(*fileBuf), meta)
		if err != nil {
			return err
		}
		f.spillETag = etag
	}
	f.dirty = false
	return nil
}

// Sync implements billy.Syncer, uploading the current contents.
func (f *writeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "sync", Path: f.name, Err: fs.ErrClosed}
	}
	return f.flush()
}

func (f *writeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	if err := f.flush(); err != nil {
		return err
	}
	f.closed = true
	f.fs.untrackWrite(f)
	f.fs.locks.releaseOnClose(f.fs.ctx, f.name, f)
	// adopt the spill into the disk cache only now: after a Sync the buffer
	// can still be written, which must not mutate a hard-linked cache entry
	if fb, ok := f.buf.(*fileBuf); ok && f.spillETag != "" {
		f.fs.disk.adopt(f.fs.key(f.name), f.spillETag, fb.path, fb.sz)
	}
	// the buffer stays alive while shared readers opened on this path are
	// still active; maybeDestroyLocked releases it once the last one closes
	f.maybeDestroyLocked()
	return nil
}

// maybeDestroyLocked releases the buffer once the writer is closed and no
// shared read handles remain. Caller must hold f.mu.
func (f *writeFile) maybeDestroyLocked() {
	if f.closed && f.shared == 0 {
		f.buf.destroy()
		f.buf = &memBuf{}
	}
}

// readAtShared reads from the live buffer regardless of handle access mode
// or closed state, for shared read handles.
func (f *writeFile) readAtShared(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if off >= f.buf.len() {
		return 0, io.EOF
	}
	return f.buf.readAt(p, off)
}

// Lock implements billy.Locker with flock-like semantics: exclusive,
// blocking, reentrant per handle and released on Close.
func (f *writeFile) Lock() error {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return &fs.PathError{Op: "lock", Path: f.name, Err: fs.ErrClosed}
	}
	if err := f.fs.locks.lock(f.fs.ctx, f.name, f); err != nil {
		return &fs.PathError{Op: "lock", Path: f.name, Err: err}
	}
	return nil
}

// Unlock implements billy.Locker; unlocking a lock not held is a no-op.
func (f *writeFile) Unlock() error {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return &fs.PathError{Op: "unlock", Path: f.name, Err: fs.ErrClosed}
	}
	if err := f.fs.locks.unlock(f.fs.ctx, f.name, f); err != nil {
		return &fs.PathError{Op: "unlock", Path: f.name, Err: err}
	}
	return nil
}

// sharedReadFile is a read-only handle over a file currently open for write
// in the same S3FS instance. Reads observe the live buffer, matching OS
// semantics for two handles on one file (required by go-git's PackWriter).
type sharedReadFile struct {
	wf *writeFile

	mu     sync.Mutex
	pos    int64
	closed bool
}

var (
	_ billy.File   = (*sharedReadFile)(nil)
	_ billy.Locker = (*sharedReadFile)(nil)
)

// newSharedReadFile attaches a read handle to a live writer. ok=false
// reports that the writer already closed, making the uploaded object
// authoritative.
func newSharedReadFile(wf *writeFile) (*sharedReadFile, bool) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	if wf.closed {
		return nil, false
	}
	wf.shared++
	return &sharedReadFile{wf: wf}, true
}

func (f *sharedReadFile) Name() string { return f.wf.name }

func (f *sharedReadFile) Stat() (fs.FileInfo, error) { return f.wf.Stat() }

func (f *sharedReadFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.wf.name, Err: fs.ErrClosed}
	}
	n, err := f.wf.readAtShared(p, f.pos)
	f.pos += int64(n)
	if n > 0 && err == io.EOF {
		// sequential reads report EOF only at the end, like os.File
		err = nil
	}
	return n, err
}

func (f *sharedReadFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed() {
		return 0, &fs.PathError{Op: "read", Path: f.wf.name, Err: fs.ErrClosed}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.wf.name, Err: fs.ErrInvalid}
	}
	return f.wf.readAtShared(p, off)
}

func (f *sharedReadFile) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *sharedReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "seek", Path: f.wf.name, Err: fs.ErrClosed}
	}
	f.wf.mu.Lock()
	size := f.wf.buf.len()
	f.wf.mu.Unlock()
	abs, err := seekPos(f.pos, size, offset, whence)
	if err != nil {
		return 0, &fs.PathError{Op: "seek", Path: f.wf.name, Err: err}
	}
	f.pos = abs
	return abs, nil
}

func (f *sharedReadFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "write", Path: f.wf.name, Err: fs.ErrPermission}
}

func (f *sharedReadFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "writeat", Path: f.wf.name, Err: fs.ErrPermission}
}

func (f *sharedReadFile) Truncate(size int64) error {
	return &fs.PathError{Op: "truncate", Path: f.wf.name, Err: fs.ErrPermission}
}

func (f *sharedReadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.wf.name, Err: fs.ErrClosed}
	}
	f.closed = true
	f.wf.mu.Lock()
	f.wf.shared--
	f.wf.maybeDestroyLocked()
	f.wf.mu.Unlock()
	f.wf.fs.locks.releaseOnClose(f.wf.fs.ctx, f.wf.name, f)
	return nil
}

// Lock implements billy.Locker with flock-like semantics; this handle
// contends with its writer and any other handle on the same path.
func (f *sharedReadFile) Lock() error {
	if f.isClosed() {
		return &fs.PathError{Op: "lock", Path: f.wf.name, Err: fs.ErrClosed}
	}
	s := f.wf.fs
	if err := s.locks.lock(s.ctx, f.wf.name, f); err != nil {
		return &fs.PathError{Op: "lock", Path: f.wf.name, Err: err}
	}
	return nil
}

// Unlock implements billy.Locker; unlocking a lock not held is a no-op.
func (f *sharedReadFile) Unlock() error {
	if f.isClosed() {
		return &fs.PathError{Op: "unlock", Path: f.wf.name, Err: fs.ErrClosed}
	}
	s := f.wf.fs
	if err := s.locks.unlock(s.ctx, f.wf.name, f); err != nil {
		return &fs.PathError{Op: "unlock", Path: f.wf.name, Err: err}
	}
	return nil
}
