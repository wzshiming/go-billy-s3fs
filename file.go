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

// writeFile buffers contents in memory and uploads to S3 on Close or Sync.
type writeFile struct {
	fs   *S3FS
	name string
	flag int
	perm fs.FileMode

	mu      sync.Mutex
	data    []byte
	pos     int64
	dirty   bool
	closed  bool
	modTime time.Time
}

func newWriteFile(s *S3FS, p string, flag int, perm fs.FileMode, data []byte, dirty bool) *writeFile {
	if perm == 0 {
		perm = defaultFileMode
	}
	f := &writeFile{
		fs:      s,
		name:    p,
		flag:    flag,
		perm:    perm.Perm(),
		data:    data,
		dirty:   dirty,
		modTime: time.Now(),
	}
	s.trackWrite(f)
	return f
}

// snapshot returns a copy of the current buffer.
func (f *writeFile) snapshot() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.data...)
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
		size:    int64(len(f.data)),
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
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
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
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *writeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrClosed}
	}
	if f.flag&os.O_APPEND != 0 {
		f.pos = int64(len(f.data))
	}
	n := f.writeAt(p, f.pos)
	f.pos += int64(n)
	return n, nil
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
	return f.writeAt(p, off), nil
}

// writeAt writes p at off, growing the buffer with zeros when needed.
// Caller must hold f.mu.
func (f *writeFile) writeAt(p []byte, off int64) int {
	if end := off + int64(len(p)); end > int64(len(f.data)) {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	}
	n := copy(f.data[off:], p)
	f.dirty = true
	f.modTime = time.Now()
	return n
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

func (f *writeFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrClosed}
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	}
	if size <= int64(len(f.data)) {
		f.data = f.data[:size]
	} else {
		grown := make([]byte, size)
		copy(grown, f.data)
		f.data = grown
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
	if err := f.fs.put(f.fs.key(f.name), f.data, meta); err != nil {
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
	// keep f.data: shared readers opened on this path may still be active
	f.fs.untrackWrite(f)
	return nil
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
		abs = int64(len(f.wf.data)) + offset
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
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
