package s3fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awshttp "github.com/aws/smithy-go/transport/http"
)

const (
	// lockOwnerMetaKey stores the random ID of the current lock holder.
	lockOwnerMetaKey = "s3fs-lock-owner"
	// lockExpiryMetaKey stores the lease deadline as unix seconds.
	lockExpiryMetaKey = "s3fs-lock-expiry"

	defaultLockPrefix = ".s3fs-lock/"
	defaultLockTTL    = 60 * time.Second
	defaultLockPoll   = time.Second
)

// S3Locker is a cross-process Locker backed by S3 conditional writes: a
// lock is an object created with If-None-Match:"*", so only one client can
// hold it. Holders renew a TTL lease in the background; locks left by a
// crashed process are taken over once the lease expires, using
// If-Match:<etag> so concurrent takeovers cannot both win.
//
// Requirements and caveats: the store must support conditional writes (AWS
// S3, MinIO, gofakes3 do); clocks must agree well within the TTL; like all
// lease-based locks it is advisory, and a holder that cannot renew (e.g.
// network outage longer than the TTL) silently loses the lock.
type S3Locker struct {
	client Client
	bucket string
	prefix string
	ttl    time.Duration
	poll   time.Duration

	mu   sync.Mutex
	held map[string]*heldLock
}

type heldLock struct {
	owner string
	stop  chan struct{}
	done  chan struct{}
}

var _ Locker = (*S3Locker)(nil)

// S3LockerOption configures an S3Locker.
type S3LockerOption func(*S3Locker)

// WithLockPrefix sets the key prefix for lock objects (default
// ".s3fs-lock/"). Keep it outside any S3FS data prefix so lock objects do
// not show up as files.
func WithLockPrefix(prefix string) S3LockerOption {
	return func(l *S3Locker) {
		prefix = strings.Trim(strings.ReplaceAll(prefix, "\\", "/"), "/")
		if prefix != "" {
			l.prefix = prefix + "/"
		}
	}
}

// WithLockTTL sets the lease duration (default 60s). Stale locks are taken
// over after ttl; holders renew every ttl/3.
func WithLockTTL(ttl time.Duration) S3LockerOption {
	return func(l *S3Locker) {
		if ttl > 0 {
			l.ttl = ttl
		}
	}
}

// WithLockPoll sets how often a blocked Lock re-checks a held lock
// (default 1s).
func WithLockPoll(poll time.Duration) S3LockerOption {
	return func(l *S3Locker) {
		if poll > 0 {
			l.poll = poll
		}
	}
}

// NewS3Locker returns a Locker storing lock objects in the given bucket.
// Multiple processes must use the same bucket and prefix for locks to be
// mutual.
func NewS3Locker(client Client, bucket string, opts ...S3LockerOption) *S3Locker {
	l := &S3Locker{
		client: client,
		bucket: bucket,
		prefix: defaultLockPrefix,
		ttl:    defaultLockTTL,
		poll:   defaultLockPoll,
		held:   make(map[string]*heldLock),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *S3Locker) key(name string) string {
	return l.prefix + strings.TrimPrefix(cleanPath(name), "/")
}

// Lock implements Locker: it blocks, polling every poll interval, until
// the lock object is created (or taken over after its lease expired) or
// ctx is done.
func (l *S3Locker) Lock(ctx context.Context, name string) error {
	key := l.key(name)
	owner := randOwnerID()
	for {
		etag, err := l.tryAcquire(ctx, key, owner)
		if err == nil {
			l.startHeartbeat(name, key, owner, etag)
			return nil
		}
		if !isPreconditionFailed(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.poll):
		}
	}
}

// tryAcquire attempts one acquisition: create the lock object, or replace
// it when its lease expired. A precondition-failed error means the lock is
// validly held by someone else.
func (l *S3Locker) tryAcquire(ctx context.Context, key, owner string) (etag string, err error) {
	out, err := l.putLock(ctx, key, owner, aws.String("*"), nil)
	if err == nil {
		return aws.ToString(out.ETag), nil
	}
	if !isPreconditionFailed(err) {
		return "", err
	}
	// held: check the lease
	head, herr := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if herr != nil {
		if isNotFound(herr) {
			// released in between; report as contended so the caller retries
			return "", err
		}
		return "", herr
	}
	expStr, _ := metaValue(head.Metadata, lockExpiryMetaKey)
	exp, perr := strconv.ParseInt(expStr, 10, 64)
	if perr == nil && time.Now().Unix() <= exp {
		return "", err // lease still valid
	}
	// expired (or unparsable): take over the exact object we inspected
	out, terr := l.putLock(ctx, key, owner, nil, head.ETag)
	if terr != nil {
		return "", terr
	}
	return aws.ToString(out.ETag), nil
}

// putLock writes the lock object with a fresh lease, conditioned on
// ifNoneMatch (creation) or ifMatch (takeover and renewal). The body holds
// owner and expiry so every write has a distinct ETag, making If-Match a
// true compare-and-swap; identical empty bodies would all share one ETag.
func (l *S3Locker) putLock(ctx context.Context, key, owner string, ifNoneMatch, ifMatch *string) (*s3.PutObjectOutput, error) {
	expiry := strconv.FormatInt(time.Now().Add(l.ttl).Unix(), 10)
	return l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(l.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader([]byte(owner + "\n" + expiry + "\n")),
		IfNoneMatch: ifNoneMatch,
		IfMatch:     ifMatch,
		Metadata: map[string]string{
			lockOwnerMetaKey:  owner,
			lockExpiryMetaKey: expiry,
		},
	})
}

// Unlock implements Locker: it stops the renewal, verifies the lock is
// still ours and deletes it. Unlocking a lock not held is a no-op.
func (l *S3Locker) Unlock(ctx context.Context, name string) error {
	l.mu.Lock()
	h := l.held[name]
	delete(l.held, name)
	l.mu.Unlock()
	if h == nil {
		return nil
	}
	close(h.stop)
	<-h.done
	key := l.key(name)
	head, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if owner, _ := metaValue(head.Metadata, lockOwnerMetaKey); owner != h.owner {
		return nil // lost to a takeover; nothing to release
	}
	_, err = l.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

func (l *S3Locker) startHeartbeat(name, key, owner, etag string) {
	h := &heldLock{owner: owner, stop: make(chan struct{}), done: make(chan struct{})}
	l.mu.Lock()
	l.held[name] = h
	l.mu.Unlock()
	go l.heartbeat(h, key, owner, etag)
}

// heartbeat renews the lease every ttl/3 until stopped. A precondition
// failure means the lock was taken over and renewal stops; other errors
// are retried on the next tick while the lease lasts.
func (l *S3Locker) heartbeat(h *heldLock, key, owner, etag string) {
	defer close(h.done)
	t := time.NewTicker(l.ttl / 3)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			out, err := l.putLock(context.Background(), key, owner, nil, aws.String(etag))
			if err != nil {
				if isPreconditionFailed(err) {
					return // lock lost
				}
				continue
			}
			etag = aws.ToString(out.ETag)
		}
	}
}

func randOwnerID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b[:])
}

func isPreconditionFailed(err error) bool {
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	var ae interface{ ErrorCode() string }
	return errors.As(err, &ae) && ae.ErrorCode() == "PreconditionFailed"
}
