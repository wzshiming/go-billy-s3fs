package s3fs

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// This file implements presigned URLs for object contents. Handing such a
// URL to a client lets it download or upload the bytes directly from/to S3,
// redirecting bulk traffic away from the process serving the filesystem.

// ErrNoPresignClient is returned by PresignGet and PresignPut when the
// filesystem was built without WithPresignClient.
var ErrNoPresignClient = errors.New("no presign client configured")

// PresignClient is the subset of the presign client used by S3FS.
// *s3.PresignClient satisfies it.
type PresignClient interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// PresignedRequest is a presigned S3 request. The URL is self-contained:
// the signature covers no headers besides the Host it implies, so any HTTP
// client can use it as is.
type PresignedRequest struct {
	Method string
	URL    string
}

// Presigner is implemented by filesystems handing out presigned URLs.
// Callers holding a billy.Filesystem can assert it to redirect transfers
// directly to the object store instead of proxying the bytes.
type Presigner interface {
	PresignGet(filename string, expiry time.Duration) (*PresignedRequest, error)
	PresignPut(filename string, expiry time.Duration) (*PresignedRequest, error)
}

var _ Presigner = (*S3FS)(nil)

// WithPresignEndpoint is an s3.NewPresignClient option that signs URLs
// against endpoint instead of the client's own: the address a server
// reaches S3 at (e.g. an in-cluster one) is often not reachable by the
// receivers of the URLs. The host is covered by the signature, so it
// cannot be rewritten afterwards.
func WithPresignEndpoint(endpoint string) func(*s3.PresignOptions) {
	return func(o *s3.PresignOptions) {
		o.ClientOptions = append(o.ClientOptions, func(co *s3.Options) {
			co.BaseEndpoint = aws.String(endpoint)
		})
	}
}

// presignOpts applies a positive expiry (otherwise the client's default, 15
// minutes for s3.PresignClient, is kept) and keeps the URL self-contained:
// default request checksums would add signed x-amz-checksum-* headers that
// every uploader would then have to send.
func presignOpts(expiry time.Duration) func(*s3.PresignOptions) {
	return func(o *s3.PresignOptions) {
		if expiry > 0 {
			o.Expires = expiry
		}
		o.ClientOptions = append(o.ClientOptions, func(co *s3.Options) {
			co.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		})
	}
}

// PresignGet returns a presigned request whose URL serves the contents of
// filename directly from S3 — the redirect counterpart of Open. Symlinks
// are followed; a missing file fails with fs.ErrNotExist and a directory
// with ErrIsDirectory. An expiry <= 0 keeps the client's default.
//
// The URL serves whatever S3 holds when it is used: contents still buffered
// in open write handles of this instance are not visible until uploaded on
// Close or Sync, and the URL keeps working for the lifetime of the
// signature even if the file is removed or rewritten in between.
func (s *S3FS) PresignGet(filename string, expiry time.Duration) (*PresignedRequest, error) {
	if s.presignClient == nil {
		return nil, ErrNoPresignClient
	}
	p, h, _, err := s.resolve("presignget", filename)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return nil, &fs.PathError{Op: "presignget", Path: filename, Err: ErrIsDirectory}
	}
	if h == nil {
		if ok, err := s.dirExists(p); err != nil {
			return nil, err
		} else if ok {
			return nil, &fs.PathError{Op: "presignget", Path: filename, Err: ErrIsDirectory}
		}
		return nil, &fs.PathError{Op: "presignget", Path: filename, Err: fs.ErrNotExist}
	}
	out, err := s.presignClient.PresignGetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(p)),
	}, presignOpts(expiry))
	if err != nil {
		return nil, err
	}
	return &PresignedRequest{Method: out.Method, URL: out.URL}, nil
}

// PresignPut returns a presigned request whose URL accepts a plain HTTP
// PUT of the contents of filename directly into S3 — the redirect
// counterpart of Create. The file need not exist; symlinks are followed so
// an upload through a link rewrites its target, and a directory fails with
// ErrIsDirectory. An expiry <= 0 keeps the client's default. Objects
// uploaded this way carry no mode metadata and appear with the default
// 0o644 mode.
//
// The upload happens outside this instance, so to the local cache tiers it
// is a write by another client. Signing forgets the cached state of the
// path, which keeps the common sign, upload, read sequence consistent even
// with a never-expiring cache; reads of the path between signing and the
// completion of the upload can cache the old state again, though, so with
// such interleavings use a finite cache ttl to bound the staleness.
func (s *S3FS) PresignPut(filename string, expiry time.Duration) (*PresignedRequest, error) {
	if s.presignClient == nil {
		return nil, ErrNoPresignClient
	}
	p, h, _, err := s.resolve("presignput", filename)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return nil, &fs.PathError{Op: "presignput", Path: filename, Err: ErrIsDirectory}
	}
	if h == nil {
		if ok, err := s.dirExists(p); err != nil {
			return nil, err
		} else if ok {
			return nil, &fs.PathError{Op: "presignput", Path: filename, Err: ErrIsDirectory}
		}
	}
	key := s.key(p)
	out, err := s.presignClient.PresignPutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, presignOpts(expiry))
	if err != nil {
		return nil, err
	}
	// the upload bypasses the write-through caches; forget the key so
	// post-upload reads fetch the fresh state from S3
	if s.mem != nil {
		s.mem.dropDeleted(key)
	}
	if s.disk != nil {
		s.disk.dropDeleted(key)
	}
	return &PresignedRequest{Method: out.Method, URL: out.URL}, nil
}
