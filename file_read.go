package s3fs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

// readBackend supplies the contents of a readOnlyFile.
type readBackend interface {
	// read serves sequential reads at off. It runs under the file mutex,
	// so implementations may keep stream state between calls.
	read(p []byte, off int64) (int, error)
	// readAt serves ReadAt: it must be safe for concurrent use and report
	// io.EOF on short reads.
	readAt(p []byte, off int64) (int, error)
	close() error
}

// readOnlyFile is a billy.File opened O_RDONLY. Contents come from a
// readBackend: streamed from S3, a body cached in memory, or a body cached
// in a local file.
type readOnlyFile struct {
	fs   *S3FS
	name string
	info fileInfo
	back readBackend

	mu     sync.Mutex
	pos    int64
	closed bool
}

var (
	_ billy.File   = (*readOnlyFile)(nil)
	_ billy.Locker = (*readOnlyFile)(nil)
)

// newReadFile returns a file streaming the object at p from S3. body, when
// non-nil, is an already-open stream at offset 0 that serves the first
// sequential reads.
func newReadFile(s *S3FS, p string, h *s3.HeadObjectOutput, body io.ReadCloser) *readOnlyFile {
	info := infoFromHeadValue(path.Base(p), h)
	return &readOnlyFile{
		fs:   s,
		name: p,
		info: info,
		back: &s3Backend{fs: s, key: s.key(p), size: info.size, body: body},
	}
}

// newMemReadFile returns a file over a body shared with the memory cache;
// data must not be mutated.
func newMemReadFile(s *S3FS, name string, info fileInfo, data []byte) *readOnlyFile {
	info.size = int64(len(data))
	return &readOnlyFile{fs: s, name: name, info: info, back: bytesBackend(data)}
}

// newDiskReadFile returns a file over a locally cached body, owning f.
func newDiskReadFile(s *S3FS, f *os.File, name string, info fileInfo) *readOnlyFile {
	return &readOnlyFile{fs: s, name: name, info: info, back: fileBackend{f}}
}

func (f *readOnlyFile) Name() string { return f.name }

func (f *readOnlyFile) Stat() (fs.FileInfo, error) {
	info := f.info
	return &info, nil
}

func (f *readOnlyFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrClosed}
	}
	if f.pos >= f.info.size {
		return 0, io.EOF
	}
	n, err := f.back.read(p, f.pos)
	f.pos += int64(n)
	if err == io.EOF && n > 0 {
		// sequential reads report EOF only once exhausted, like os.File
		err = nil
	}
	return n, err
}

func (f *readOnlyFile) ReadAt(p []byte, off int64) (int, error) {
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
	return f.back.readAt(p, off)
}

func (f *readOnlyFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrClosed}
	}
	abs, err := seekPos(f.pos, f.info.size, offset, whence)
	if err != nil {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: err}
	}
	f.pos = abs
	return abs, nil
}

func (f *readOnlyFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
}

func (f *readOnlyFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: fs.ErrPermission}
}

func (f *readOnlyFile) Truncate(size int64) error {
	return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrPermission}
}

func (f *readOnlyFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	f.fs.locks.releaseOnClose(f.fs.ctx, f.name, f)
	return f.back.close()
}

func (f *readOnlyFile) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// Lock implements billy.Locker with flock-like semantics: exclusive,
// blocking, reentrant per handle and released on Close.
func (f *readOnlyFile) Lock() error {
	if f.isClosed() {
		return &fs.PathError{Op: "lock", Path: f.name, Err: fs.ErrClosed}
	}
	if err := f.fs.locks.lock(f.fs.ctx, f.name, f); err != nil {
		return &fs.PathError{Op: "lock", Path: f.name, Err: err}
	}
	return nil
}

// Unlock implements billy.Locker; unlocking a lock not held is a no-op.
func (f *readOnlyFile) Unlock() error {
	if f.isClosed() {
		return &fs.PathError{Op: "unlock", Path: f.name, Err: fs.ErrClosed}
	}
	if err := f.fs.locks.unlock(f.fs.ctx, f.name, f); err != nil {
		return &fs.PathError{Op: "unlock", Path: f.name, Err: err}
	}
	return nil
}

// seekPos resolves a Seek to an absolute position, rejecting negatives.
func seekPos(pos, size, offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = pos + offset
	case io.SeekEnd:
		abs = size + offset
	default:
		return 0, fs.ErrInvalid
	}
	if abs < 0 {
		return 0, fs.ErrInvalid
	}
	return abs, nil
}

// s3Backend streams object contents from S3. Sequential reads share one
// ranged-GET body; readAt issues an independent ranged GET per call, so it
// is safe for concurrent use.
type s3Backend struct {
	fs   *S3FS
	key  string
	size int64

	body    io.ReadCloser // sequential stream positioned at bodyPos, may be nil
	bodyPos int64
}

func (b *s3Backend) read(p []byte, off int64) (int, error) {
	if b.body != nil && b.bodyPos != off {
		b.body.Close()
		b.body = nil
	}
	if b.body == nil {
		out, err := b.fs.client.GetObject(b.fs.ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.fs.bucket),
			Key:    aws.String(b.key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-", off)),
		})
		if err != nil {
			return 0, err
		}
		b.body = out.Body
		b.bodyPos = off
	}
	n, err := b.body.Read(p)
	b.bodyPos += int64(n)
	if err == io.EOF && b.bodyPos < b.size {
		// stream ended early; force reopen on the next read
		b.body.Close()
		b.body = nil
		err = nil
	}
	return n, err
}

func (b *s3Backend) readAt(p []byte, off int64) (int, error) {
	out, err := b.fs.client.GetObject(b.fs.ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.fs.bucket),
		Key:    aws.String(b.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	n, err := io.ReadFull(out.Body, p)
	if err == io.ErrUnexpectedEOF || (err == nil && n < len(p)) {
		err = io.EOF
	}
	return n, err
}

func (b *s3Backend) close() error {
	if b.body != nil {
		err := b.body.Close()
		b.body = nil
		return err
	}
	return nil
}

// bytesBackend serves a body shared with the memory cache; it must not be
// mutated.
type bytesBackend []byte

func (b bytesBackend) read(p []byte, off int64) (int, error) { return b.readAt(p, off) }

func (b bytesBackend) readAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b bytesBackend) close() error { return nil }

// fileBackend serves a locally cached body with pread, owning the file.
type fileBackend struct{ f *os.File }

func (b fileBackend) read(p []byte, off int64) (int, error) { return b.f.ReadAt(p, off) }

func (b fileBackend) readAt(p []byte, off int64) (int, error) { return b.f.ReadAt(p, off) }

func (b fileBackend) close() error { return b.f.Close() }
