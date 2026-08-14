package s3fs

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
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

// PresignedRequest is a presigned S3 request. With an empty SignedHeader
// the URL is self-contained: the signature covers no headers besides the
// Host it implies, so any HTTP client can use it as is.
type PresignedRequest struct {
	Method string
	URL    string

	// SignedHeader holds the headers the signature covers beyond the Host
	// implied by the URL; the request must send them verbatim or S3
	// rejects it. It is nil unless an option signed one — plain URLs stay
	// plain — e.g. the x-amz-checksum-sha256 header of WithContentSHA256.
	SignedHeader http.Header
}

// Presigner is implemented by filesystems handing out presigned URLs.
// Callers holding a billy.Filesystem can assert it to redirect transfers
// directly to the object store instead of proxying the bytes.
type Presigner interface {
	PresignGet(filename string, opts ...PresignGetOption) (*PresignedRequest, error)
	PresignPut(filename string, opts ...PresignPutOption) (*PresignedRequest, error)
}

var _ Presigner = (*S3FS)(nil)

// presignConfig collects the per-call options of PresignGet and PresignPut.
type presignConfig struct {
	expiry        time.Duration
	contentSHA256 string
}

// PresignGetOption adjusts what a single PresignGet call signs. Every
// PresignOption is one.
type PresignGetOption interface {
	applyPresignGet(*presignConfig)
}

// PresignPutOption adjusts what a single PresignPut call signs: every
// PresignOption is one, and some, like WithContentSHA256, apply only here.
type PresignPutOption interface {
	applyPresignPut(*presignConfig)
}

// PresignOption is a presign option accepted by PresignGet and PresignPut
// alike, like WithExpiry.
type PresignOption interface {
	PresignGetOption
	PresignPutOption
}

type expiryOption time.Duration

func (e expiryOption) applyPresignGet(c *presignConfig) { c.expiry = time.Duration(e) }
func (e expiryOption) applyPresignPut(c *presignConfig) { c.expiry = time.Duration(e) }

// WithExpiry makes the signature valid for d instead of the client's
// default, 15 minutes for s3.PresignClient. A d <= 0 keeps the default.
func WithExpiry(d time.Duration) PresignOption {
	return expiryOption(d)
}

type contentSHA256Option string

func (o contentSHA256Option) applyPresignPut(c *presignConfig) { c.contentSHA256 = string(o) }

// WithContentSHA256 ties a PresignPut grant to its expected content: sum
// is the SHA256 of the bytes to be uploaded, either 64 hex chars (the
// sha256sum and git-LFS OID format) or the base64 of the 32 raw digest
// bytes (the S3 ChecksumSHA256 format, which hex is re-encoded to at sign
// time). S3 then rejects any upload whose body does not hash to sum. The
// digest travels as the signed x-amz-checksum-sha256 header, so the
// upload is no longer a bare PUT: the headers in
// PresignedRequest.SignedHeader must be sent with it.
func WithContentSHA256(sum string) PresignPutOption {
	return contentSHA256Option(sum)
}

// normalizeSHA256 brings a digest in either accepted encoding into the
// base64 form S3 validates, catching malformed digests at sign time
// instead of as an S3 error on the upload holder's side.
func normalizeSHA256(sum string) (string, error) {
	if len(sum) == hex.EncodedLen(sha256.Size) {
		if d, err := hex.DecodeString(sum); err == nil {
			return base64.StdEncoding.EncodeToString(d), nil
		}
		// 64 chars can still be valid base64, though never of 32 bytes
	}
	if d, err := base64.StdEncoding.DecodeString(sum); err == nil && len(d) == sha256.Size {
		return sum, nil
	}
	return "", fmt.Errorf("content sha256 %q: want 64 hex chars or the base64 of the 32 raw digest bytes", sum)
}

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
// every uploader would then have to send. A checksum the caller asks for
// explicitly (WithContentSHA256) is serialized from the input and still
// gets signed.
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

// extraSignedHeader returns the signed headers a request must carry beyond
// host, which the URL implies and every HTTP client sends on its own; nil
// when there are none, keeping plain URLs plain.
func extraSignedHeader(h http.Header) http.Header {
	var extra http.Header
	for k, vs := range h {
		if strings.EqualFold(k, "host") {
			continue
		}
		if extra == nil {
			extra = http.Header{}
		}
		for _, v := range vs {
			extra.Add(k, v)
		}
	}
	return extra
}

// withSignedChecksumHeader signs x-amz-* headers in place instead of
// hoisting them into the query string. S3 enforces a content checksum only
// when it arrives as a header, so hoisting would turn the pinned digest
// into an inert query parameter. Used only when a checksum is requested;
// plain URLs keep the default hoisting and stay header-free.
func withSignedChecksumHeader(o *s3.PresignOptions) {
	o.Presigner = v4.NewSigner(func(so *v4.SignerOptions) {
		so.DisableURIPathEscaping = true // S3 signs the raw path
		so.DisableHeaderHoisting = true
	})
}

// PresignGet returns a presigned request whose URL serves the contents of
// filename directly from S3 — the redirect counterpart of Open. Symlinks
// are followed; a missing file fails with fs.ErrNotExist and a directory
// with ErrIsDirectory. WithExpiry adjusts how long the URL stays valid.
//
// The URL serves whatever S3 holds when it is used: contents still buffered
// in open write handles of this instance are not visible until uploaded on
// Close or Sync, and the URL keeps working for the lifetime of the
// signature even if the file is removed or rewritten in between.
func (s *S3FS) PresignGet(filename string, opts ...PresignGetOption) (*PresignedRequest, error) {
	if s.presignClient == nil {
		return nil, ErrNoPresignClient
	}
	var cfg presignConfig
	for _, o := range opts {
		o.applyPresignGet(&cfg)
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
	}, presignOpts(cfg.expiry))
	if err != nil {
		return nil, err
	}
	return &PresignedRequest{Method: out.Method, URL: out.URL, SignedHeader: extraSignedHeader(out.SignedHeader)}, nil
}

// PresignPut returns a presigned request whose URL accepts a plain HTTP
// PUT of the contents of filename directly into S3 — the redirect
// counterpart of Create. The file need not exist; symlinks are followed so
// an upload through a link rewrites its target, and a directory fails with
// ErrIsDirectory. WithExpiry adjusts how long the URL stays valid. Objects
// uploaded this way carry no mode metadata and appear with the default
// 0o644 mode.
//
// Without options the URL accepts whatever the holder uploads; options
// like WithContentSHA256 narrow the grant. Options that sign headers cost
// the URL its self-containedness: the uploader must then send the headers
// returned in SignedHeader.
//
// The upload happens outside this instance, so to the local cache tiers it
// is a write by another client. Signing forgets the cached state of the
// path, which keeps the common sign, upload, read sequence consistent even
// with a never-expiring cache; reads of the path between signing and the
// completion of the upload can cache the old state again, though, so with
// such interleavings use a finite cache ttl to bound the staleness.
func (s *S3FS) PresignPut(filename string, opts ...PresignPutOption) (*PresignedRequest, error) {
	if s.presignClient == nil {
		return nil, ErrNoPresignClient
	}
	var cfg presignConfig
	for _, o := range opts {
		o.applyPresignPut(&cfg)
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
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	popts := []func(*s3.PresignOptions){presignOpts(cfg.expiry)}
	if cfg.contentSHA256 != "" {
		sum, err := normalizeSHA256(cfg.contentSHA256)
		if err != nil {
			return nil, &fs.PathError{Op: "presignput", Path: filename, Err: err}
		}
		in.ChecksumSHA256 = aws.String(sum)
		popts = append(popts, withSignedChecksumHeader)
	}
	out, err := s.presignClient.PresignPutObject(s.ctx, in, popts...)
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
	return &PresignedRequest{Method: out.Method, URL: out.URL, SignedHeader: extraSignedHeader(out.SignedHeader)}, nil
}
