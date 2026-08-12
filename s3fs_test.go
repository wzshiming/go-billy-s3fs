package s3fs_test

import (
	"errors"
	"io"
	"io/fs"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

const testBucket = "test-bucket"

// newTestClient starts an in-memory fake S3 server and returns a client for it.
func newTestClient(t testing.TB) *s3.Client {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket(testBucket); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)

	return s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(server.URL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

func newTestFS(t testing.TB, opts ...s3fs.Option) *s3fs.S3FS {
	t.Helper()
	return s3fs.New(newTestClient(t), testBucket, opts...)
}

// fsVariants are the option sets shared suites run against: the plain
// filesystem and one with the local write-through cache enabled.
var fsVariants = []struct {
	name string
	opts []s3fs.Option
}{
	{"plain", nil},
	{"cached", []s3fs.Option{s3fs.WithCache(64<<20, 0)}},
}

func writeFull(t *testing.T, bfs billy.Filesystem, name, content string) {
	t.Helper()
	if err := util.WriteFile(bfs, name, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func readFull(t *testing.T, bfs billy.Filesystem, name string) string {
	t.Helper()
	data, err := util.ReadFile(bfs, name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func TestCreateWriteRead(t *testing.T) {
	bfs := newTestFS(t)

	f, err := bfs.Create("dir/sub/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if got := readFull(t, bfs, "dir/sub/hello.txt"); got != "hello world" {
		t.Fatalf("content = %q, want %q", got, "hello world")
	}

	fi, err := bfs.Stat("dir/sub/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(len("hello world")) || fi.IsDir() {
		t.Fatalf("unexpected stat: size=%d isdir=%v", fi.Size(), fi.IsDir())
	}
	if fi.Name() != "hello.txt" {
		t.Fatalf("name = %q", fi.Name())
	}

	// implied parent directories exist
	for _, dir := range []string{"dir", "dir/sub", "/"} {
		fi, err := bfs.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !fi.IsDir() {
			t.Fatalf("%s should be a dir", dir)
		}
	}
}

func TestCreateEmptyFile(t *testing.T) {
	bfs := newTestFS(t)
	f, err := bfs.Create("empty")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := bfs.Stat("empty")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("size = %d", fi.Size())
	}
}

func TestOpenFlags(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "f", "0123456789")

	t.Run("not exist", func(t *testing.T) {
		if _, err := bfs.Open("missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("err = %v, want ErrNotExist", err)
		}
		if _, err := bfs.Stat("missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat err = %v, want ErrNotExist", err)
		}
	})

	t.Run("excl", func(t *testing.T) {
		if _, err := bfs.OpenFile("f", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("err = %v, want ErrExist", err)
		}
		f, err := bfs.OpenFile("new-excl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	})

	t.Run("append", func(t *testing.T) {
		f, err := bfs.OpenFile("f", os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("ab")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, bfs, "f"); got != "0123456789ab" {
			t.Fatalf("content = %q", got)
		}
	})

	t.Run("overwrite without trunc keeps tail", func(t *testing.T) {
		f, err := bfs.OpenFile("f", os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, bfs, "f"); got != "XY23456789ab" {
			t.Fatalf("content = %q", got)
		}
	})

	t.Run("trunc", func(t *testing.T) {
		f, err := bfs.OpenFile("f", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("z")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got := readFull(t, bfs, "f"); got != "z" {
			t.Fatalf("content = %q", got)
		}
	})

	t.Run("write to read-only handle", func(t *testing.T) {
		f, err := bfs.Open("f")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.Write([]byte("nope")); err == nil {
			t.Fatal("write on O_RDONLY should fail")
		}
	})

	t.Run("open dir", func(t *testing.T) {
		writeFull(t, bfs, "d/x", "1")
		if _, err := bfs.Open("d"); err == nil {
			t.Fatal("open dir should fail")
		}
	})
}

func TestSeekAndReadAt(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "data", "0123456789")

	f, err := bfs.Open("data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if pos, err := f.Seek(4, io.SeekStart); err != nil || pos != 4 {
		t.Fatalf("seek: %d %v", pos, err)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(f, buf); err != nil || string(buf) != "456" {
		t.Fatalf("read after seek: %q %v", buf, err)
	}

	if pos, err := f.Seek(-2, io.SeekEnd); err != nil || pos != 8 {
		t.Fatalf("seek end: %d %v", pos, err)
	}
	rest, err := io.ReadAll(f)
	if err != nil || string(rest) != "89" {
		t.Fatalf("read to end: %q %v", rest, err)
	}

	// concurrent ReadAt on a shared handle
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			b := make([]byte, 2)
			if n, err := f.ReadAt(b, off); err != nil || n != 2 {
				t.Errorf("ReadAt(%d): %d %v", off, n, err)
				return
			}
			want := "0123456789"[off : off+2]
			if string(b) != want {
				t.Errorf("ReadAt(%d) = %q, want %q", off, b, want)
			}
		}(int64(i))
	}
	wg.Wait()

	// EOF conditions
	b := make([]byte, 4)
	if n, err := f.ReadAt(b, 8); err != io.EOF || n != 2 {
		t.Fatalf("ReadAt over end: n=%d err=%v", n, err)
	}
	if _, err := f.ReadAt(b, 100); err != io.EOF {
		t.Fatalf("ReadAt past end: %v", err)
	}
}

func TestTruncate(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "t", "0123456789")

	f, err := bfs.OpenFile("t", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(6); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, bfs, "t"); got != "0123\x00\x00" {
		t.Fatalf("content = %q", got)
	}
}

func TestReadDir(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "a.txt", "1")
	writeFull(t, bfs, "b/c.txt", "2")
	writeFull(t, bfs, "b/d/e.txt", "3")
	if err := bfs.MkdirAll("empty", 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := bfs.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	isDir := map[string]bool{}
	for _, e := range entries {
		names = append(names, e.Name())
		isDir[e.Name()] = e.IsDir()
	}
	want := []string{"a.txt", "b", "empty"}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("entries not sorted: %v", names)
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("names = %v, want %v", names, want)
	}
	if isDir["a.txt"] || !isDir["b"] || !isDir["empty"] {
		t.Fatalf("wrong types: %v", isDir)
	}

	sub, err := bfs.ReadDir("b")
	if err != nil {
		t.Fatal(err)
	}
	if len(sub) != 2 || sub[0].Name() != "c.txt" || sub[1].Name() != "d" {
		t.Fatalf("sub = %v", sub)
	}

	if list, err := bfs.ReadDir("empty"); err != nil || len(list) != 0 {
		t.Fatalf("empty dir: %v %v", list, err)
	}

	if _, err := bfs.ReadDir("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing dir err = %v", err)
	}
	if _, err := bfs.ReadDir("a.txt"); err == nil {
		t.Fatal("ReadDir on file should fail")
	}
}

func TestMkdirAll(t *testing.T) {
	bfs := newTestFS(t)
	if err := bfs.MkdirAll("x/y/z", 0o755); err != nil {
		t.Fatal(err)
	}
	// idempotent
	if err := bfs.MkdirAll("x/y/z", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"x", "x/y", "x/y/z"} {
		fi, err := bfs.Stat(dir)
		if err != nil || !fi.IsDir() {
			t.Fatalf("stat %s: %v %v", dir, fi, err)
		}
	}
	writeFull(t, bfs, "file", "1")
	if err := bfs.MkdirAll("file", 0o755); err == nil {
		t.Fatal("MkdirAll over file should fail")
	}
}

func TestRename(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "src.txt", "content")

	if err := bfs.Rename("src.txt", "moved/dst.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := bfs.Stat("src.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("src still exists: %v", err)
	}
	if got := readFull(t, bfs, "moved/dst.txt"); got != "content" {
		t.Fatalf("content = %q", got)
	}

	// directory rename
	writeFull(t, bfs, "dir/a", "A")
	writeFull(t, bfs, "dir/sub/b", "B")
	if err := bfs.Rename("dir", "dir2"); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, bfs, "dir2/a"); got != "A" {
		t.Fatalf("dir2/a = %q", got)
	}
	if got := readFull(t, bfs, "dir2/sub/b"); got != "B" {
		t.Fatalf("dir2/sub/b = %q", got)
	}
	if _, err := bfs.Stat("dir"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old dir still exists: %v", err)
	}

	if err := bfs.Rename("missing", "anywhere"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rename missing: %v", err)
	}
}

func TestRemove(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "f", "1")
	writeFull(t, bfs, "d/child", "2")
	if err := bfs.MkdirAll("emptyd", 0o755); err != nil {
		t.Fatal(err)
	}

	if err := bfs.Remove("f"); err != nil {
		t.Fatal(err)
	}
	if _, err := bfs.Stat("f"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("f should be gone")
	}

	if err := bfs.Remove("d"); err == nil {
		t.Fatal("removing non-empty dir should fail")
	}
	if err := bfs.Remove("emptyd"); err != nil {
		t.Fatalf("removing empty dir: %v", err)
	}
	if err := bfs.Remove("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("remove missing: %v", err)
	}
}

func TestSymlink(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "target.txt", "real content")

	if err := bfs.Symlink("target.txt", "link.txt"); err != nil {
		t.Fatal(err)
	}

	got, err := bfs.Readlink("link.txt")
	if err != nil || got != "target.txt" {
		t.Fatalf("readlink = %q, %v", got, err)
	}

	fi, err := bfs.Lstat("link.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&fs.ModeSymlink == 0 {
		t.Fatalf("lstat mode = %v, want symlink", fi.Mode())
	}

	// Stat and Open follow the link
	fi, err = bfs.Stat("link.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		t.Fatal("stat should follow symlink")
	}
	if got := readFull(t, bfs, "link.txt"); got != "real content" {
		t.Fatalf("content through link = %q", got)
	}

	// relative target in subdirectory
	writeFull(t, bfs, "sub/data", "sub data")
	if err := bfs.Symlink("data", "sub/rel-link"); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, bfs, "sub/rel-link"); got != "sub data" {
		t.Fatalf("relative link content = %q", got)
	}

	// writing through a symlink updates the target
	writeFull(t, bfs, "link.txt", "updated")
	if got := readFull(t, bfs, "target.txt"); got != "updated" {
		t.Fatalf("target after write-through = %q", got)
	}

	if err := bfs.Symlink("x", "link.txt"); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("symlink over existing: %v", err)
	}
	if _, err := bfs.Readlink("target.txt"); err == nil {
		t.Fatal("readlink on regular file should fail")
	}
}

func TestTempFile(t *testing.T) {
	bfs := newTestFS(t)
	f1, err := bfs.TempFile("tmp", "prefix-")
	if err != nil {
		t.Fatal(err)
	}
	f2, err := bfs.TempFile("tmp", "prefix-")
	if err != nil {
		t.Fatal(err)
	}
	if f1.Name() == f2.Name() {
		t.Fatalf("temp files collide: %s", f1.Name())
	}
	if !strings.Contains(f1.Name(), "prefix-") {
		t.Fatalf("name = %q", f1.Name())
	}
	if _, err := f1.Write([]byte("tmp data")); err != nil {
		t.Fatal(err)
	}
	if err := f1.Close(); err != nil {
		t.Fatal(err)
	}
	f2.Close()
	if got := readFull(t, bfs, f1.Name()); got != "tmp data" {
		t.Fatalf("content = %q", got)
	}
}

func TestChroot(t *testing.T) {
	bfs := newTestFS(t)
	sub, err := bfs.Chroot("some/dir")
	if err != nil {
		t.Fatal(err)
	}
	writeFull(t, sub, "file.txt", "chrooted")
	if got := readFull(t, bfs, "some/dir/file.txt"); got != "chrooted" {
		t.Fatalf("content = %q", got)
	}
	if sub.Root() != "/some/dir" {
		t.Fatalf("root = %q", sub.Root())
	}
}

func TestWithPrefix(t *testing.T) {
	client := newTestClient(t)
	fsA := s3fs.New(client, testBucket, s3fs.WithPrefix("tenant-a"))
	fsB := s3fs.New(client, testBucket, s3fs.WithPrefix("tenant-b"))
	root := s3fs.New(client, testBucket)

	writeFull(t, fsA, "cfg", "A")
	writeFull(t, fsB, "cfg", "B")

	if got := readFull(t, fsA, "cfg"); got != "A" {
		t.Fatalf("fsA cfg = %q", got)
	}
	if got := readFull(t, root, "tenant-a/cfg"); got != "A" {
		t.Fatalf("root view = %q", got)
	}
	if got := readFull(t, root, "tenant-b/cfg"); got != "B" {
		t.Fatalf("root view = %q", got)
	}
}

func TestCapabilities(t *testing.T) {
	bfs := newTestFS(t)
	caps := billy.Capabilities(bfs)
	need := billy.WriteCapability | billy.ReadCapability |
		billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
	if caps&need != need {
		t.Fatalf("caps = %b", caps)
	}
	if caps&billy.LockCapability != 0 {
		t.Fatal("lock capability should not be advertised")
	}
}

func TestFileStatAndMode(t *testing.T) {
	bfs := newTestFS(t)
	f, err := bfs.OpenFile("exec.sh", os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("#!/bin/sh")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := bfs.Stat("exec.sh")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("mode = %v, want 0755", fi.Mode())
	}

	rf, err := bfs.Open("exec.sh")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	fi2, err := rf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() != int64(len("#!/bin/sh")) {
		t.Fatalf("file stat size = %d", fi2.Size())
	}
}
