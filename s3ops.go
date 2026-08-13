package s3fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awshttp "github.com/aws/smithy-go/transport/http"
	"github.com/go-git/go-billy/v6"
)

// This file holds the S3 request primitives of S3FS. Every S3 round trip is
// made here, keeping the cache write-through bookkeeping (mem, disk) in one
// place; the billy.Filesystem methods in s3fs.go build on these.

// headFromGet builds a HeadObjectOutput from a GetObject response, letting
// symlink detection, file info and the mem cache treat both uniformly.
func headFromGet(out *s3.GetObjectOutput) *s3.HeadObjectOutput {
	return &s3.HeadObjectOutput{
		ContentLength: out.ContentLength,
		ETag:          out.ETag,
		LastModified:  out.LastModified,
		Metadata:      out.Metadata,
	}
}

func (s *S3FS) head(key string) (*s3.HeadObjectOutput, error) {
	if s.mem != nil {
		if e, ok := s.mem.lookup(key); ok {
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
	if s.mem != nil {
		if err == nil {
			s.mem.store(key, true, out, nil)
		} else if isNotFound(err) {
			s.mem.store(key, false, nil, nil)
		}
	}
	return out, err
}

func (s *S3FS) put(key string, data []byte, meta map[string]string) error {
	out, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: meta,
	})
	if err != nil {
		return err
	}
	if s.mem != nil {
		body := data
		if body == nil {
			body = []byte{}
		}
		s.mem.storeWrite(key, int64(len(data)), aws.ToString(out.ETag), body, meta)
	}
	if s.disk != nil {
		s.disk.dropDeleted(key) // overwrite invalidates any cached body
	}
	return nil
}

// putSpill uploads a spilled write buffer and returns the new ETag. The
// caller adopts the file into the disk cache once no further writes can
// reach it; adopting here would let later writes through the same handle
// mutate the hard-linked cache entry in place.
func (s *S3FS) putSpill(key string, fb *fileBuf, meta map[string]string) (string, error) {
	if _, err := fb.f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	out, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          fb.f,
		ContentLength: aws.Int64(fb.sz),
		Metadata:      meta,
	})
	if err != nil {
		return "", err
	}
	etag := aws.ToString(out.ETag)
	if s.mem != nil {
		s.mem.storeWrite(key, fb.sz, etag, nil, meta)
	}
	if s.disk != nil {
		s.disk.dropDeleted(key) // the upload replaced any cached body
	}
	return etag, nil
}

// spillThreshold is the write-buffer size above which contents move to a
// spill file when the disk cache is enabled.
func (s *S3FS) spillThreshold() int64 {
	if s.mem != nil {
		return s.mem.maxDataBytes()
	}
	return defaultSpillLimit
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

// bufForExisting loads the current contents of p for a read-modify-write
// open: into memory when small, streamed into a spill file when the disk
// cache is enabled and the object is large.
func (s *S3FS) bufForExisting(p string, h *s3.HeadObjectOutput) (writeBuf, error) {
	key := s.key(p)
	if s.disk == nil || aws.ToInt64(h.ContentLength) <= s.spillThreshold() {
		data, err := s.getAll(key)
		if err != nil {
			return nil, err
		}
		return &memBuf{data: data}, nil
	}
	fb, err := s.disk.newSpill()
	if err != nil {
		data, gerr := s.getAll(key) // disk unusable; fall back to memory
		if gerr != nil {
			return nil, gerr
		}
		return &memBuf{data: data}, nil
	}
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		fb.destroy()
		return nil, err
	}
	defer out.Body.Close()
	n, err := io.Copy(fb.f, out.Body)
	if err != nil {
		fb.destroy()
		return nil, err
	}
	fb.sz = n
	return fb, nil
}

// bufFromGet is bufForExisting fed by the body of the GetObject that
// resolved the path, saving the separate download.
func (s *S3FS) bufFromGet(h *s3.HeadObjectOutput, g *s3.GetObjectOutput) (writeBuf, error) {
	defer g.Body.Close()
	if s.disk == nil || aws.ToInt64(h.ContentLength) <= s.spillThreshold() {
		data, err := io.ReadAll(g.Body)
		if err != nil {
			return nil, err
		}
		return &memBuf{data: data}, nil
	}
	fb, err := s.disk.newSpill()
	if err != nil {
		data, rerr := io.ReadAll(g.Body) // disk unusable; fall back to memory
		if rerr != nil {
			return nil, rerr
		}
		return &memBuf{data: data}, nil
	}
	n, err := io.Copy(fb.f, g.Body)
	if err != nil {
		fb.destroy()
		return nil, err
	}
	fb.sz = n
	return fb, nil
}

func (s *S3FS) delete(key string) error {
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err == nil && s.mem != nil {
		s.mem.dropDeleted(key)
	}
	if err == nil && s.disk != nil {
		s.disk.dropDeleted(key)
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
	if err == nil && s.mem != nil {
		s.mem.storeCopied(srcKey, dstKey)
	}
	if err == nil && s.disk != nil {
		s.disk.copied(srcKey, dstKey)
	}
	return err
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

// dirExists reports whether p exists as a directory (marker object or any
// key under its prefix).
func (s *S3FS) dirExists(p string) (bool, error) {
	if cleanPath(p) == "/" {
		return true, nil
	}
	prefix := s.listPrefix(p)
	if s.mem != nil {
		if e, ok := s.mem.lookup(prefix); ok {
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
	// servers may return an empty page with IsTruncated set instead of
	// filling MaxKeys, which still proves a key under the prefix exists
	exists := len(out.Contents) > 0 || aws.ToBool(out.IsTruncated)
	if s.mem != nil {
		s.mem.store(prefix, exists, nil, nil)
	}
	return exists, nil
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

// resolveForRead is resolve for opens that need the object's contents: on a
// cold cache each hop is fetched with a single GetObject instead of
// HeadObject followed by a later GetObject. The returned GetObjectOutput,
// when non-nil, has its body open at offset 0 and is owned by the caller;
// it is nil when the metadata came from the local tiers (the body, if any,
// is served by cachedOpen) or the path did not resolve to an object.
func (s *S3FS) resolveForRead(op, p string) (string, *s3.HeadObjectOutput, *s3.GetObjectOutput, int, error) {
	p = cleanPath(p)
	hops := 0
	for {
		if p == "/" {
			return p, nil, nil, hops, nil
		}
		h, g, err := s.headOrGet(s.key(p))
		if err != nil {
			if isNotFound(err) {
				return p, nil, nil, hops, nil
			}
			return p, nil, nil, hops, err
		}
		target, ok := symlinkTarget(h)
		if !ok {
			return p, h, g, hops, nil
		}
		if g != nil {
			g.Body.Close() // symlink bodies are empty
		}
		hops++
		if hops > maxSymlinkDepth {
			return p, nil, nil, hops, &fs.PathError{Op: op, Path: p, Err: ErrTooManySymlinks}
		}
		if path.IsAbs(target) {
			p = path.Clean(target)
		} else {
			p = path.Join(path.Dir(p), target)
		}
	}
}

// headOrGet returns the object metadata for key, issuing a single GetObject
// when nothing local can serve it; the returned GetObjectOutput then
// carries the open body. It stays nil when the metadata was cached, or when
// the disk cache holds a body for key, which a cheap HeadObject validates
// instead of re-downloading the object.
func (s *S3FS) headOrGet(key string) (*s3.HeadObjectOutput, *s3.GetObjectOutput, error) {
	if s.mem != nil {
		if e, ok := s.mem.lookup(key); ok {
			if !e.exists {
				return nil, nil, &types.NotFound{}
			}
			if e.head != nil {
				return e.head, nil, nil
			}
		}
	}
	if s.disk != nil && s.disk.has(key) {
		h, err := s.head(key)
		return h, nil, err
	}
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if s.mem != nil && isNotFound(err) {
			s.mem.store(key, false, nil, nil)
		}
		return nil, nil, err
	}
	h := headFromGet(out)
	if s.mem != nil {
		s.mem.store(key, true, h, nil)
	}
	return h, out, nil
}

// cachedOpen serves an O_RDONLY open from the local tiers: the memory cache
// for bodies that fit it, the disk cache for anything larger. ok=false
// means the caller should stream from S3.
func (s *S3FS) cachedOpen(p string, h *s3.HeadObjectOutput) (billy.File, bool, error) {
	size := aws.ToInt64(h.ContentLength)
	key := s.key(p)
	if s.mem != nil && size <= s.mem.maxDataBytes() {
		if e, ok := s.mem.lookup(key); ok && e.exists && e.data != nil {
			return newMemReadFile(p, infoFromHeadValue(path.Base(p), h), e.data), true, nil
		}
		data, err := s.getAll(key)
		if err != nil {
			if isNotFound(err) {
				return nil, false, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist}
			}
			return nil, false, err
		}
		s.mem.store(key, true, h, data)
		return newMemReadFile(p, infoFromHeadValue(path.Base(p), h), data), true, nil
	}
	if s.disk != nil && size+entryOverhead <= s.disk.maxBytes {
		if f, ok := s.disk.open(key, aws.ToString(h.ETag)); ok {
			info := infoFromHeadValue(path.Base(p), h)
			return newDiskReadFile(f, p, info), true, nil
		}
		return s.diskFetch(key, h, p)
	}
	return nil, false, nil
}

// openFromGet serves an O_RDONLY open from the body of the GetObject that
// resolved the path: small bodies land in the memory cache, medium ones
// stream into the disk cache, and anything else keeps the open body as the
// sequential read stream.
func (s *S3FS) openFromGet(p string, h *s3.HeadObjectOutput, g *s3.GetObjectOutput) (billy.File, error) {
	size := aws.ToInt64(h.ContentLength)
	key := s.key(p)
	if s.mem != nil && size <= s.mem.maxDataBytes() {
		data, err := io.ReadAll(g.Body)
		g.Body.Close()
		if err != nil {
			return nil, err
		}
		s.mem.store(key, true, h, data)
		return newMemReadFile(p, infoFromHeadValue(path.Base(p), h), data), nil
	}
	if s.disk != nil && size+entryOverhead <= s.disk.maxBytes {
		f, ok, err := s.diskFill(key, aws.ToString(h.ETag), h, p, g.Body)
		if err != nil {
			g.Body.Close()
			return nil, err
		}
		if ok {
			g.Body.Close()
			return f, nil
		}
		// disk unusable; keep streaming from the open body
	}
	return newReadFile(s, p, h, g.Body), nil
}

// diskFetch streams the object into the disk cache and returns a handle
// serving it. ok=false means the caller should stream from S3 instead.
func (s *S3FS) diskFetch(key string, h *s3.HeadObjectOutput, p string) (billy.File, bool, error) {
	if s.disk.ensure() != nil {
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
	etag := aws.ToString(out.ETag)
	if etag == "" {
		etag = aws.ToString(h.ETag)
	}
	return s.diskFill(key, etag, h, p, out.Body)
}

// diskFill streams body into a new disk cache entry for key and returns a
// handle serving it. ok=false with a nil error means the disk is unusable
// and body was not consumed; the caller should stream instead.
func (s *S3FS) diskFill(key, etag string, h *s3.HeadObjectOutput, p string, body io.Reader) (billy.File, bool, error) {
	c := s.disk
	if c.ensure() != nil {
		return nil, false, nil
	}
	tmp := c.newPath("c")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, nil // disk unusable; stream instead
	}
	size, err := io.Copy(f, body)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, false, err
	}
	info := infoFromHeadValue(path.Base(p), h)
	info.size = size
	if etag == "" {
		// cannot validate later; serve this handle but do not cache
		os.Remove(tmp)
		return newDiskReadFile(f, p, info), true, nil
	}
	c.insert(key, etag, tmp, size)
	return newDiskReadFile(f, p, info), true, nil
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
