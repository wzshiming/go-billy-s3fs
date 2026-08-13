package s3fs_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// countingClient wraps an s3fs.API and counts calls per operation.
type countingClient struct {
	s3fs.Client
	mu    sync.Mutex
	calls map[string]int
}

func newCountingClient(inner s3fs.Client) *countingClient {
	return &countingClient{Client: inner, calls: map[string]int{}}
}

func (c *countingClient) record(op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[op]++
}

func (c *countingClient) count(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[op]
}

func (c *countingClient) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

func (c *countingClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.record("HeadObject")
	return c.Client.HeadObject(ctx, in, opts...)
}

func (c *countingClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.record("GetObject")
	return c.Client.GetObject(ctx, in, opts...)
}

func (c *countingClient) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.record("PutObject")
	return c.Client.PutObject(ctx, in, opts...)
}

func (c *countingClient) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.record("DeleteObject")
	return c.Client.DeleteObject(ctx, in, opts...)
}

func (c *countingClient) CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	c.record("CopyObject")
	return c.Client.CopyObject(ctx, in, opts...)
}

func (c *countingClient) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.record("ListObjectsV2")
	return c.Client.ListObjectsV2(ctx, in, opts...)
}

// TestCacheReadAfterWrite verifies write-through caching: after a write via
// the filesystem, reads and stats are served without any S3 round trips.
func TestCacheReadAfterWrite(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	bfs := s3fs.New(testBucket, s3fs.WithClient(client), s3fs.WithMemCache(64<<20, 0))

	writeFull(t, bfs, "dir/a.txt", "hello cache")

	before := client.total()
	for range 3 {
		if got := readFull(t, bfs, "dir/a.txt"); got != "hello cache" {
			t.Fatalf("content = %q", got)
		}
		fi, err := bfs.Stat("dir/a.txt")
		if err != nil || fi.Size() != int64(len("hello cache")) {
			t.Fatalf("stat: %v, %v", fi, err)
		}
	}
	if n := client.total() - before; n != 0 {
		t.Fatalf("S3 calls after write-through = %d, want 0", n)
	}
}

// TestCacheNegativeStat verifies missing paths are cached and that the
// negative entry does not mask a later create.
func TestCacheNegativeStat(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	bfs := s3fs.New(testBucket, s3fs.WithClient(client), s3fs.WithMemCache(64<<20, 0))

	if _, err := bfs.Stat("missing.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("first stat err = %v", err)
	}
	before := client.total()
	if _, err := bfs.Stat("missing.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("second stat err = %v", err)
	}
	if n := client.total() - before; n != 0 {
		t.Fatalf("S3 calls for cached negative = %d, want 0", n)
	}

	writeFull(t, bfs, "missing.txt", "now exists")
	if got := readFull(t, bfs, "missing.txt"); got != "now exists" {
		t.Fatalf("content after create = %q", got)
	}
}

// TestCacheInvalidation verifies Rename and Remove keep the cache coherent.
func TestCacheInvalidation(t *testing.T) {
	bfs := newTestFS(t, s3fs.WithMemCache(64<<20, 0))

	writeFull(t, bfs, "a.txt", "v1")
	_ = readFull(t, bfs, "a.txt") // populate body cache

	if err := bfs.Rename("a.txt", "b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := bfs.Stat("a.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat old after rename: %v", err)
	}
	if got := readFull(t, bfs, "b.txt"); got != "v1" {
		t.Fatalf("renamed content = %q", got)
	}

	if err := bfs.Remove("b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := bfs.Stat("b.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat after remove: %v", err)
	}
}

// TestCacheTTL verifies the visibility rules for writes made by an external
// client: invisible forever with ttl=0, visible after expiry otherwise.
func TestCacheTTL(t *testing.T) {
	raw := newTestClient(t)
	extPut := func(t *testing.T, key, content string) {
		t.Helper()
		if _, err := raw.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("zero ttl keeps entries", func(t *testing.T) {
		bfs := s3fs.New(testBucket, s3fs.WithClient(raw), s3fs.WithPrefix("forever"), s3fs.WithMemCache(64<<20, 0))
		writeFull(t, bfs, "f", "v1")
		extPut(t, "forever/f", "v2") // bypasses this instance's cache
		if got := readFull(t, bfs, "f"); got != "v1" {
			t.Fatalf("read = %q, want cached v1", got)
		}
	})

	t.Run("expired entries refetch", func(t *testing.T) {
		bfs := s3fs.New(testBucket, s3fs.WithClient(raw), s3fs.WithPrefix("expiring"), s3fs.WithMemCache(64<<20, time.Nanosecond))
		writeFull(t, bfs, "f", "v1")
		extPut(t, "expiring/f", "v2")
		time.Sleep(time.Millisecond) // let the entry expire
		if got := readFull(t, bfs, "f"); got != "v2" {
			t.Fatalf("read = %q, want refetched v2", got)
		}
	})
}

// TestCacheBigObjectStreams verifies bodies over the per-object limit are
// not held in memory and keep streaming from S3.
func TestCacheBigObjectStreams(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	// 4KiB budget: bodies over 512B always stream
	bfs := s3fs.New(testBucket, s3fs.WithClient(client), s3fs.WithMemCache(4096, 0))

	big := strings.Repeat("x", 10_000)
	writeFull(t, bfs, "big.bin", big)

	before := client.count("GetObject")
	if got := readFull(t, bfs, "big.bin"); got != big {
		t.Fatalf("big content mismatch: len = %d", len(got))
	}
	if client.count("GetObject") == before {
		t.Fatal("big object should stream from S3, no GetObject calls seen")
	}
}

// TestOpenSingleGetObject verifies that on a cold cache an existing file is
// opened with a single GetObject and no HeadObject, for both read-only and
// read-modify-write opens, including through symlinks.
func TestOpenSingleGetObject(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	bfs := s3fs.New(testBucket, s3fs.WithClient(client))

	writeFull(t, bfs, "dir/a.txt", "hello")
	if err := bfs.Symlink("a.txt", "dir/link"); err != nil {
		t.Fatal(err)
	}

	t.Run("read-only", func(t *testing.T) {
		heads, gets := client.count("HeadObject"), client.count("GetObject")
		if got := readFull(t, bfs, "dir/a.txt"); got != "hello" {
			t.Fatalf("content = %q", got)
		}
		if h, g := client.count("HeadObject")-heads, client.count("GetObject")-gets; h != 0 || g != 1 {
			t.Fatalf("open+read = %d HeadObject + %d GetObject, want 0 + 1", h, g)
		}
	})

	t.Run("read-write", func(t *testing.T) {
		heads, gets := client.count("HeadObject"), client.count("GetObject")
		f, err := bfs.OpenFile("dir/a.txt", os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(f)
		if err != nil || string(data) != "hello" {
			t.Fatalf("read = %q, %v", data, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if h, g := client.count("HeadObject")-heads, client.count("GetObject")-gets; h != 0 || g != 1 {
			t.Fatalf("open+read = %d HeadObject + %d GetObject, want 0 + 1", h, g)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		heads, gets := client.count("HeadObject"), client.count("GetObject")
		if got := readFull(t, bfs, "dir/link"); got != "hello" {
			t.Fatalf("content = %q", got)
		}
		if h, g := client.count("HeadObject")-heads, client.count("GetObject")-gets; h != 0 || g != 2 {
			t.Fatalf("open+read via symlink = %d HeadObject + %d GetObject, want 0 + 2", h, g)
		}
	})
}
