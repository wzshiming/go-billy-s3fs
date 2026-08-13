package s3fs_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// newLockerPair returns two S3FS instances that share one bucket but have
// independent S3Lockers, simulating two processes.
func newLockerPair(t *testing.T, ttl, poll time.Duration) (*s3.Client, *s3fs.S3FS, *s3fs.S3FS) {
	t.Helper()
	client := newTestClient(t)
	mk := func() *s3fs.S3FS {
		return s3fs.New(client, testBucket,
			s3fs.WithPrefix("repo"),
			s3fs.WithLocker(s3fs.NewS3Locker(client, testBucket,
				s3fs.WithLockTTL(ttl), s3fs.WithLockPoll(poll))))
	}
	return client, mk(), mk()
}

func headLockObject(t *testing.T, client *s3.Client, name string) (*s3.HeadObjectOutput, error) {
	t.Helper()
	return client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(".s3fs-lock/" + name),
	})
}

func TestS3LockerCrossInstance(t *testing.T) {
	client, fsA, fsB := newLockerPair(t, 10*time.Second, 20*time.Millisecond)
	writeFull(t, fsA, "/a.txt", "content")

	a, err := fsA.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := fsB.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	mustLock(t, a)
	if _, err := headLockObject(t, client, "a.txt"); err != nil {
		t.Fatalf("lock object missing while held: %v", err)
	}

	ch := lockAsync(t, b)
	assertBlocked(t, ch)
	mustUnlock(t, a)
	assertAcquired(t, ch)
	mustUnlock(t, b)

	if _, err := headLockObject(t, client, "a.txt"); err == nil {
		t.Fatal("lock object still present after final unlock")
	}
}

func TestS3LockerReleasedOnClose(t *testing.T) {
	_, fsA, fsB := newLockerPair(t, 10*time.Second, 20*time.Millisecond)
	writeFull(t, fsA, "/a.txt", "content")

	a, err := fsA.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	mustLock(t, a)

	b, err := fsB.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ch := lockAsync(t, b)
	assertBlocked(t, ch)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	assertAcquired(t, ch)
	mustUnlock(t, b)
}

// TestS3LockerStaleTakeover plants a lock object whose lease expired, as a
// crashed process would leave behind, and expects Lock to take it over.
func TestS3LockerStaleTakeover(t *testing.T) {
	client := newTestClient(t)
	expired := strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(".s3fs-lock/a.txt"),
		Body:   bytes.NewReader([]byte("dead\n" + expired + "\n")),
		Metadata: map[string]string{
			"s3fs-lock-owner":  "dead",
			"s3fs-lock-expiry": expired,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	locker := s3fs.NewS3Locker(client, testBucket,
		s3fs.WithLockTTL(10*time.Second), s3fs.WithLockPoll(20*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	if err := locker.Lock(ctx, "/a.txt"); err != nil {
		t.Fatalf("takeover of expired lock: %v", err)
	}
	if err := locker.Unlock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}
}

// TestS3LockerHeartbeat holds a lock well past its TTL and expects the
// background renewal to keep contenders out.
func TestS3LockerHeartbeat(t *testing.T) {
	client := newTestClient(t)
	mk := func() *s3fs.S3Locker {
		return s3fs.NewS3Locker(client, testBucket,
			s3fs.WithLockTTL(300*time.Millisecond), s3fs.WithLockPoll(20*time.Millisecond))
	}
	holder, contender := mk(), mk()

	if err := holder.Lock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}
	// outlive the initial lease several times over
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := contender.Lock(ctx, "/a.txt"); !errors.Is(err, context.DeadlineExceeded) {
		if err == nil {
			t.Fatal("contender acquired a lock that should be renewed")
		}
		t.Fatalf("contender: %v", err)
	}
	if err := holder.Unlock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}

	// after release the contender gets in immediately
	ctx2, cancel2 := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel2()
	if err := contender.Lock(ctx2, "/a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := contender.Unlock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}
}

// TestS3LockerCtxCancel aborts a blocked Lock through the filesystem
// context.
func TestS3LockerCtxCancel(t *testing.T) {
	client := newTestClient(t)
	locker := s3fs.NewS3Locker(client, testBucket,
		s3fs.WithLockTTL(10*time.Second), s3fs.WithLockPoll(20*time.Millisecond))
	if err := locker.Lock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fsB := s3fs.New(client, testBucket, s3fs.WithContext(ctx),
		s3fs.WithLocker(s3fs.NewS3Locker(client, testBucket,
			s3fs.WithLockTTL(10*time.Second), s3fs.WithLockPoll(20*time.Millisecond))))
	writeFull(t, fsB, "/a.txt", "content")
	b, err := fsB.Open("/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ch := lockAsync(t, b)
	assertBlocked(t, ch)
	cancel()
	select {
	case err := <-ch:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Lock after cancel: got %v, want context.Canceled", err)
		}
	case <-time.After(lockTimeout):
		t.Fatal("Lock still blocked after context cancel")
	}
	if err := locker.Unlock(context.Background(), "/a.txt"); err != nil {
		t.Fatal(err)
	}
}
