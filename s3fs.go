// Package s3fs provides a billy.Filesystem implementation backed by Amazon S3
// (or any S3-compatible object store), usable as storage for go-git v6.
//
// Layout: each file maps to an object key, directories are represented by
// zero-byte marker objects with a trailing "/" plus the implicit prefixes of
// existing keys. Symlinks are stored as empty objects with the target kept in
// object metadata. Writes are buffered in memory and uploaded on Close/Sync;
// reads stream from S3 using ranged requests, so ReadAt is safe for
// concurrent use as required by billy.File.
//
// An optional in-memory cache (WithCache) keeps object metadata, negative
// lookups, directory existence and small object bodies local, cutting S3
// round trips dramatically for read-heavy workloads such as go-git. The
// cache is write-through: every mutation made through the same instance
// updates it immediately, so read-after-write stays consistent. Writes made
// by other clients are only observed after entries expire (the ttl given to
// WithCache; a ttl of zero never expires and assumes a single writer).
package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awshttp "github.com/aws/smithy-go/transport/http"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
)

const (
	// symlinkMetaKey stores the URL-encoded symlink target in object metadata.
	symlinkMetaKey = "s3fs-symlink-target"
	// modeMetaKey stores the file permission bits in octal.
	modeMetaKey = "s3fs-mode"

	maxSymlinkDepth = 40

	defaultFileMode = fs.FileMode(0o644)
	defaultDirMode  = fs.ModeDir | 0o755
)

var (
	ErrIsDirectory     = errors.New("is a directory")
	ErrNotADirectory   = errors.New("not a directory")
	ErrDirNotEmpty     = errors.New("directory not empty")
	ErrTooManySymlinks = errors.New("too many levels of symbolic links")
)

// API is the subset of the S3 client used by S3FS. *s3.Client satisfies it.
type API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3FS is a billy.Filesystem backed by an S3 bucket.
type S3FS struct {
	client API
	bucket string
	prefix string
	ctx    context.Context

	// writesMu guards openWrites, which tracks files opened for write whose
	// contents are not yet uploaded. Readers of the same path within this
	// instance observe the live buffer, mirroring OS filesystem semantics
	// (required by go-git's PackWriter, which reads a temp file while
	// writing it).
	writesMu   sync.Mutex
	openWrites map[string]*writeFile

	// cache, when non-nil, is a local write-through layer over S3: it serves
	// HeadObject results, negative lookups, directory existence and small
	// object bodies, and is updated by every mutation done through this
	// instance.
	cache *objCache
}

var (
	_ billy.Filesystem = (*S3FS)(nil)
	_ billy.Capable    = (*S3FS)(nil)
)

// Option configures an S3FS.
type Option func(*S3FS)

// WithPrefix roots the filesystem at the given key prefix inside the bucket.
func WithPrefix(prefix string) Option {
	return func(s *S3FS) {
		s.prefix = strings.Trim(path.Clean("/"+strings.ReplaceAll(prefix, "\\", "/")), "/")
	}
}

// WithContext sets the context used for all S3 requests.
func WithContext(ctx context.Context) Option {
	return func(s *S3FS) { s.ctx = ctx }
}

// WithCache enables a local in-memory cache holding up to maxBytes of
// metadata and object bodies (bodies larger than maxBytes/8 always stream).
// The cache is write-through for mutations made via this instance; ttl
// bounds how long writes from other clients can stay invisible. A ttl <= 0
// keeps entries forever, which is only safe when this instance is the sole
// writer of the bucket prefix.
func WithCache(maxBytes int64, ttl time.Duration) Option {
	return func(s *S3FS) {
		if maxBytes > 0 {
			s.cache = newObjCache(maxBytes, ttl)
		}
	}
}

// New returns a billy filesystem stored in the given bucket.
func New(client API, bucket string, opts ...Option) *S3FS {
	s := &S3FS{
		client:     client,
		bucket:     bucket,
		ctx:        context.Background(),
		openWrites: make(map[string]*writeFile),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *S3FS) trackWrite(f *writeFile) {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	s.openWrites[f.name] = f
}

func (s *S3FS) untrackWrite(f *writeFile) {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	if s.openWrites[f.name] == f {
		delete(s.openWrites, f.name)
	}
}

func (s *S3FS) lookupWrite(p string) *writeFile {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	return s.openWrites[p]
}

// Capabilities implements billy.Capable. File locks are accepted but are
// no-ops, so LockCapability is not advertised (matching memfs).
func (s *S3FS) Capabilities() billy.Capability {
	return billy.WriteCapability |
		billy.ReadCapability |
		billy.ReadAndWriteCapability |
		billy.SeekCapability |
		billy.TruncateCapability
}

// cleanPath normalizes p to an absolute slash-separated path.
func cleanPath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// key maps an absolute billy path to an object key.
func (s *S3FS) key(p string) string {
	rel := strings.TrimPrefix(cleanPath(p), "/")
	switch {
	case s.prefix == "":
		return rel
	case rel == "":
		return s.prefix
	default:
		return s.prefix + "/" + rel
	}
}

// listPrefix returns the key prefix that contains the children of p.
func (s *S3FS) listPrefix(p string) string {
	k := s.key(p)
	if k == "" {
		return ""
	}
	return k + "/"
}

func (s *S3FS) head(key string) (*s3.HeadObjectOutput, error) {
	if s.cache != nil {
		if e, ok := s.cache.lookup(key); ok {
			if !e.exists {
				return nil, &types.NotFound{}
			}
			if e.head != nil {
				return e.head, nil
			}
		}
	}
	out, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if s.cache != nil {
		if err == nil {
			s.cache.store(key, true, out, nil)
		} else if isNotFound(err) {
			s.cache.store(key, false, nil, nil)
		}
	}
	return out, err
}

func (s *S3FS) put(key string, data []byte, meta map[string]string) error {
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: meta,
	})
	if err == nil && s.cache != nil {
		s.cache.storeWrite(key, data, meta)
	}
	return err
}

func (s *S3FS) getAll(key string) ([]byte, error) {
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3FS) delete(key string) error {
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err == nil && s.cache != nil {
		s.cache.dropDeleted(key)
	}
	return err
}

func (s *S3FS) copy(srcKey, dstKey string) error {
	segs := strings.Split(srcKey, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	_, err := s.client.CopyObject(s.ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(s.bucket + "/" + strings.Join(segs, "/")),
	})
	if err == nil && s.cache != nil {
		s.cache.storeCopied(srcKey, dstKey)
	}
	return err
}

func isNotFound(err error) bool {
	var nf *types.NotFound
	var nsk *types.NoSuchKey
	if errors.As(err, &nf) || errors.As(err, &nsk) {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}

// metaValue looks up a metadata key case-insensitively; S3 SDKs and
// providers do not agree on the returned casing.
func metaValue(meta map[string]string, key string) (string, bool) {
	if v, ok := meta[key]; ok {
		return v, true
	}
	for k, v := range meta {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

func symlinkTarget(h *s3.HeadObjectOutput) (string, bool) {
	if h == nil {
		return "", false
	}
	enc, ok := metaValue(h.Metadata, symlinkMetaKey)
	if !ok {
		return "", false
	}
	target, err := url.QueryUnescape(enc)
	if err != nil {
		return "", false
	}
	return target, true
}

// resolve follows symlinks on the final path component. It returns the
// resolved path, the object head if the path exists as an object, and the
// number of symlink hops taken.
func (s *S3FS) resolve(op, p string) (string, *s3.HeadObjectOutput, int, error) {
	p = cleanPath(p)
	hops := 0
	for {
		if p == "/" {
			return p, nil, hops, nil
		}
		h, err := s.head(s.key(p))
		if err != nil {
			if isNotFound(err) {
				return p, nil, hops, nil
			}
			return p, nil, hops, err
		}
		target, ok := symlinkTarget(h)
		if !ok {
			return p, h, hops, nil
		}
		hops++
		if hops > maxSymlinkDepth {
			return p, nil, hops, &fs.PathError{Op: op, Path: p, Err: ErrTooManySymlinks}
		}
		if path.IsAbs(target) {
			p = path.Clean(target)
		} else {
			p = path.Join(path.Dir(p), target)
		}
	}
}

// dirExists reports whether p exists as a directory (marker object or any
// key under its prefix).
func (s *S3FS) dirExists(p string) (bool, error) {
	if cleanPath(p) == "/" {
		return true, nil
	}
	prefix := s.listPrefix(p)
	if s.cache != nil {
		if e, ok := s.cache.lookup(prefix); ok {
			return e.exists, nil
		}
	}
	out, err := s.client.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false, err
	}
	exists := len(out.Contents) > 0
	if s.cache != nil {
		s.cache.store(prefix, exists, nil, nil)
	}
	return exists, nil
}

// Create implements billy.Basic.
func (s *S3FS) Create(filename string) (billy.File, error) {
	return s.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, defaultFileMode)
}

// Open implements billy.Basic.
func (s *S3FS) Open(filename string) (billy.File, error) {
	return s.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile implements billy.Basic. Parent directories are not required to
// exist; missing ones are implied, following object store semantics.
func (s *S3FS) OpenFile(filename string, flag int, perm fs.FileMode) (billy.File, error) {
	// a file being written in this instance is visible before upload
	if wf := s.lookupWrite(cleanPath(filename)); wf != nil {
		if flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL {
			return nil, &fs.PathError{Op: "open", Path: filename, Err: fs.ErrExist}
		}
		if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
			return newSharedReadFile(wf), nil
		}
		var data []byte
		if flag&os.O_TRUNC == 0 {
			data = wf.snapshot()
		}
		return newWriteFile(s, cleanPath(filename), flag, perm, data, true), nil
	}

	p, h, hops, err := s.resolve("open", filename)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return nil, &fs.PathError{Op: "open", Path: filename, Err: ErrIsDirectory}
	}
	if flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL && (h != nil || hops > 0) {
		return nil, &fs.PathError{Op: "open", Path: filename, Err: fs.ErrExist}
	}

	writable := flag&(os.O_WRONLY|os.O_RDWR) != 0
	if h != nil {
		if !writable {
			if f, ok, err := s.cachedOpen(p, h); err != nil {
				return nil, err
			} else if ok {
				return f, nil
			}
			return newReadFile(s, p, h), nil
		}
		var data []byte
		if flag&os.O_TRUNC == 0 {
			data, err = s.getAll(s.key(p))
			if err != nil {
				if isNotFound(err) {
					return nil, &fs.PathError{Op: "open", Path: filename, Err: fs.ErrNotExist}
				}
				return nil, err
			}
		}
		return newWriteFile(s, p, flag, perm, data, flag&os.O_TRUNC != 0), nil
	}

	if ok, err := s.dirExists(p); err != nil {
		return nil, err
	} else if ok {
		return nil, &fs.PathError{Op: "open", Path: filename, Err: ErrIsDirectory}
	}
	if flag&os.O_CREATE == 0 {
		return nil, &fs.PathError{Op: "open", Path: filename, Err: fs.ErrNotExist}
	}
	return newWriteFile(s, p, flag, perm, nil, true), nil
}

// Stat implements billy.Basic, following symlinks.
func (s *S3FS) Stat(filename string) (fs.FileInfo, error) {
	if wf := s.lookupWrite(cleanPath(filename)); wf != nil {
		return wf.Stat()
	}
	p, h, _, err := s.resolve("stat", filename)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return &fileInfo{name: "/", mode: defaultDirMode}, nil
	}
	if h != nil {
		return infoFromHead(path.Base(p), h), nil
	}
	if ok, err := s.dirExists(p); err != nil {
		return nil, err
	} else if ok {
		return &fileInfo{name: path.Base(p), mode: defaultDirMode}, nil
	}
	return nil, &fs.PathError{Op: "stat", Path: filename, Err: fs.ErrNotExist}
}

// Lstat implements billy.Symlink, not following symlinks.
func (s *S3FS) Lstat(filename string) (fs.FileInfo, error) {
	p := cleanPath(filename)
	if p == "/" {
		return &fileInfo{name: "/", mode: defaultDirMode}, nil
	}
	h, err := s.head(s.key(p))
	if err == nil {
		if target, ok := symlinkTarget(h); ok {
			return &fileInfo{
				name:    path.Base(p),
				size:    int64(len(target)),
				mode:    fs.ModeSymlink | 0o777,
				modTime: aws.ToTime(h.LastModified),
			}, nil
		}
		return infoFromHead(path.Base(p), h), nil
	}
	if !isNotFound(err) {
		return nil, err
	}
	if ok, err := s.dirExists(p); err != nil {
		return nil, err
	} else if ok {
		return &fileInfo{name: path.Base(p), mode: defaultDirMode}, nil
	}
	return nil, &fs.PathError{Op: "lstat", Path: filename, Err: fs.ErrNotExist}
}

// Symlink implements billy.Symlink. The target is stored in object metadata.
func (s *S3FS) Symlink(target, link string) error {
	p := cleanPath(link)
	if p == "/" {
		return &os.LinkError{Op: "symlink", Old: target, New: link, Err: fs.ErrExist}
	}
	if _, err := s.head(s.key(p)); err == nil {
		return &os.LinkError{Op: "symlink", Old: target, New: link, Err: fs.ErrExist}
	} else if !isNotFound(err) {
		return err
	}
	if ok, err := s.dirExists(p); err != nil {
		return err
	} else if ok {
		return &os.LinkError{Op: "symlink", Old: target, New: link, Err: fs.ErrExist}
	}
	return s.put(s.key(p), nil, map[string]string{symlinkMetaKey: url.QueryEscape(target)})
}

// Readlink implements billy.Symlink.
func (s *S3FS) Readlink(link string) (string, error) {
	p := cleanPath(link)
	h, err := s.head(s.key(p))
	if err != nil {
		if isNotFound(err) {
			return "", &fs.PathError{Op: "readlink", Path: link, Err: fs.ErrNotExist}
		}
		return "", err
	}
	target, ok := symlinkTarget(h)
	if !ok {
		return "", &fs.PathError{Op: "readlink", Path: link, Err: fs.ErrInvalid}
	}
	return target, nil
}

// Rename implements billy.Basic. Files are moved with server-side copy;
// directories move all keys under their prefix.
func (s *S3FS) Rename(oldpath, newpath string) error {
	op, np := cleanPath(oldpath), cleanPath(newpath)
	if op == "/" {
		return billy.ErrBaseDirCannotBeRenamed
	}
	if np == "/" {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrExist}
	}
	if op == np {
		if _, err := s.head(s.key(op)); err == nil {
			return nil
		} else if !isNotFound(err) {
			return err
		}
		if ok, err := s.dirExists(op); err != nil {
			return err
		} else if ok {
			return nil
		}
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}

	if _, err := s.head(s.key(op)); err == nil {
		if err := s.copy(s.key(op), s.key(np)); err != nil {
			return err
		}
		return s.delete(s.key(op))
	} else if !isNotFound(err) {
		return err
	}

	if ok, err := s.dirExists(op); err != nil {
		return err
	} else if !ok {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}
	if strings.HasPrefix(np+"/", op+"/") {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrInvalid}
	}
	keys, err := s.listAllKeys(s.listPrefix(op))
	if err != nil {
		return err
	}
	oldPrefix, newPrefix := s.listPrefix(op), s.listPrefix(np)
	for _, k := range keys {
		if err := s.copy(k, newPrefix+strings.TrimPrefix(k, oldPrefix)); err != nil {
			return err
		}
	}
	for _, k := range keys {
		if err := s.delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3FS) listAllKeys(prefix string) ([]string, error) {
	var keys []string
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	for {
		out, err := s.client.ListObjectsV2(s.ctx, in)
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		in.ContinuationToken = out.NextContinuationToken
	}
	return keys, nil
}

// Remove implements billy.Basic. Removing a non-empty directory fails with
// ErrDirNotEmpty.
func (s *S3FS) Remove(filename string) error {
	p := cleanPath(filename)
	if p == "/" {
		return billy.ErrBaseDirCannotBeRemoved
	}
	if _, err := s.head(s.key(p)); err == nil {
		return s.delete(s.key(p))
	} else if !isNotFound(err) {
		return err
	}

	prefix := s.listPrefix(p)
	out, err := s.client.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		return err
	}
	if len(out.Contents) == 0 {
		return &fs.PathError{Op: "remove", Path: filename, Err: fs.ErrNotExist}
	}
	for _, obj := range out.Contents {
		if aws.ToString(obj.Key) != prefix {
			return &fs.PathError{Op: "remove", Path: filename, Err: ErrDirNotEmpty}
		}
	}
	return s.delete(prefix)
}

// Join implements billy.Basic.
func (s *S3FS) Join(elem ...string) string {
	return path.Join(elem...)
}

// TempFile implements billy.TempFile.
func (s *S3FS) TempFile(dir, prefix string) (billy.File, error) {
	return util.TempFile(s, dir, prefix)
}

// ReadDir implements billy.Dir. Symlinks are listed as regular files because
// S3 listings carry no metadata; use Lstat for accurate types.
func (s *S3FS) ReadDir(p string) ([]fs.DirEntry, error) {
	rp, h, _, err := s.resolve("open", p)
	if err != nil {
		return nil, err
	}
	if h != nil {
		return nil, &fs.PathError{Op: "open", Path: p, Err: ErrNotADirectory}
	}

	prefix := s.listPrefix(rp)
	byName := make(map[string]*dirEntry)
	found := false
	in := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	for {
		out, err := s.client.ListObjectsV2(s.ctx, in)
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			found = true
			name := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
			if name == "" {
				continue // marker of the listed directory itself
			}
			if dir, ok := strings.CutSuffix(name, "/"); ok {
				// backends may report child dir markers in Contents
				if dir != "" && byName[dir] == nil {
					byName[dir] = &dirEntry{fileInfo{name: dir, mode: defaultDirMode}}
				}
				continue
			}
			byName[name] = &dirEntry{fileInfo{
				name:    name,
				size:    aws.ToInt64(obj.Size),
				mode:    defaultFileMode,
				modTime: aws.ToTime(obj.LastModified),
			}}
		}
		for _, cp := range out.CommonPrefixes {
			found = true
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), prefix), "/")
			if name == "" || byName[name] != nil {
				continue
			}
			byName[name] = &dirEntry{fileInfo{name: name, mode: defaultDirMode}}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		in.ContinuationToken = out.NextContinuationToken
	}
	if !found && rp != "/" {
		return nil, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist}
	}
	entries := make([]fs.DirEntry, 0, len(byName))
	for _, e := range byName {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

// MkdirAll implements billy.Dir by writing a zero-byte directory marker for
// the leaf; ancestors are implied by the key prefix.
func (s *S3FS) MkdirAll(p string, perm fs.FileMode) error {
	rp, h, _, err := s.resolve("mkdir", p)
	if err != nil {
		return err
	}
	if rp == "/" {
		return nil
	}
	if h != nil {
		return &fs.PathError{Op: "mkdir", Path: p, Err: ErrNotADirectory}
	}
	if ok, err := s.dirExists(rp); err != nil {
		return err
	} else if ok {
		return nil
	}
	return s.put(s.listPrefix(rp), nil, nil)
}

// Chroot implements billy.Chroot.
func (s *S3FS) Chroot(p string) (billy.Filesystem, error) {
	return chroot.New(s, cleanPath(p)), nil
}

// Root implements billy.Chroot.
func (s *S3FS) Root() string {
	return "/"
}
