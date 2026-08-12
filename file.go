package s3fs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

var (
	_ billy.File   = (*readFile)(nil)
	_ billy.File   = (*writeFile)(nil)
	_ billy.Locker = (*readFile)(nil)
	_ billy.Locker = (*writeFile)(nil)
	_ billy.Syncer = (*writeFile)(nil)
)

// readFile streams an object opened O_RDONLY. Sequential reads share one
// ranged GET body; ReadAt issues independent ranged GETs, so it is safe for
// concurrent use.
type readFile struct {
	fs   *S3FS
	name string
	info fileInfo

	mu      sync.Mutex
	pos     int64
	body    io.ReadCloser // sequential stream positioned at pos, may be nil
	bodyPos int64
	closed  bool
}

func newReadFile(s *S3FS, p string, h *s3.HeadObjectOutput) *readFile {
	return &readFile{fs: s, name: p, info: infoFromHeadValue(path.Base(p), h)}
}

func (f *readFile) Name() string { return f.name }

func (f *readFile) Stat() (fs.FileInfo, error) {
	info := f.info
	return &info, nil
}

func (f *readFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if f.pos >= f.info.size {
		return 0, io.EOF
	}
	if f.body != nil && f.bodyPos != f.pos {
		f.body.Close()
		f.body = nil
	}
	if f.body == nil {
		out, err := f.fs.client.GetObject(f.fs.ctx, &s3.GetObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.fs.key(f.name)),
			Range:  aws.String(fmt.Sprintf("bytes=%d-", f.pos)),
		})
		if err != nil {
			return 0, err
		}
		f.body = out.Body
		f.bodyPos = f.pos
	}
	n, err := f.body.Read(p)
	f.pos += int64(n)
	f.bodyPos = f.pos
	if err == io.EOF && f.pos < f.info.size {
		// stream ended early; force reopen on next read
		f.body.Close()
		f.body = nil
		err = nil
	}
	return n, err
}

func (f *readFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed() {
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
	out, err := f.fs.client.GetObject(f.fs.ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.fs.key(f.name)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	n, err := io.ReadFull(out.Body, p)
	if err == io.ErrUnexpectedEOF || (err == nil && int64(n) < int64(len(p))) {
		err = io.EOF
	}
	return n, err
}

func (f *readFile) Seek(offset int64, whence int) (int64, error) {
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

func (f *readFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
}

func (f *readFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: fs.ErrPermission}
}

func (f *readFile) Truncate(size int64) error {
	return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
}

func (f *readFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	if f.body != nil {
		err := f.body.Close()
		f.body = nil
		return err
	}
	return nil
}

func (f *readFile) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// Lock implements billy.Locker as a no-op.
func (f *readFile) Lock() error { return nil }

// Unlock implements billy.Locker as a no-op.
func (f *readFile) Unlock() error { return nil }

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
	if end := off + int64(len(p)); end > int64(len(b.data)) {
		grown := make([]byte, end)
		copy(grown, b.data)
		b.data = grown
	}
	return copy(b.data[off:], p), nil
}

func (b *memBuf) truncate(size int64) error {
	if size <= int64(len(b.data)) {
		b.data = b.data[:size]
	} else {
		grown := make([]byte, size)
		copy(grown, b.data)
		b.data = grown
	}
	return nil
}

func (b *memBuf) bytes() ([]byte, bool) { return b.data, true }

func (b *memBuf) snapshot() ([]byte, error) { return append([]byte(nil), b.data...), nil }

func (b *memBuf) destroy() {}

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
}

func newWriteFile(s *S3FS, p string, flag int, perm fs.FileMode, data []byte, dirty bool) *writeFile {
	return newWriteFileBuf(s, p, flag, perm, &memBuf{data: data}, dirty)
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
	f.buf = fb
}

// snapshot returns a copy of the current buffer.
func (f *writeFile) snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.snapshot()
}

func (f *writeFile) Name() string { return f.name }

func (f *writeFile) readable() bool {
	return f.flag&(os.O_WRONLY|os.O_RDWR) != os.O_WRONLY
}

func (f *writeFile) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &fileInfo{
		name:    path.Base(f.name),
		size:    f.buf.len(),
		mode:    f.perm,
		modTime: f.modTime,
	}, nil
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
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		abs = f.buf.len() + offset
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
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
	var err error
	if data, ok := f.buf.bytes(); ok {
		err = f.fs.put(f.fs.key(f.name), data, meta)
	} else {
		err = f.fs.putSpill(f.fs.key(f.name), f.buf.(*fileBuf), meta)
	}
	if err != nil {
		return err
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
	// the buffer stays alive while shared readers opened on this path are
	// still active; maybeDestroyLocked releases it once the last one closes
	f.fs.untrackWrite(f)
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

// Lock implements billy.Locker as a no-op.
func (f *writeFile) Lock() error { return nil }

// Unlock implements billy.Locker as a no-op.
func (f *writeFile) Unlock() error { return nil }

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

func newSharedReadFile(wf *writeFile) *sharedReadFile {
	wf.mu.Lock()
	wf.shared++
	wf.mu.Unlock()
	return &sharedReadFile{wf: wf}
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
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.wf.name, Err: fs.ErrInvalid}
	}
	return f.wf.readAtShared(p, off)
}

func (f *sharedReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "seek", Path: f.wf.name, Err: fs.ErrClosed}
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		f.wf.mu.Lock()
		abs = f.wf.buf.len() + offset
		f.wf.mu.Unlock()
	default:
		return 0, &fs.PathError{Op: "seek", Path: f.wf.name, Err: fs.ErrInvalid}
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.wf.name, Err: fs.ErrInvalid}
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
	return nil
}

// Lock implements billy.Locker as a no-op.
func (f *sharedReadFile) Lock() error { return nil }

// Unlock implements billy.Locker as a no-op.
func (f *sharedReadFile) Unlock() error { return nil }

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
