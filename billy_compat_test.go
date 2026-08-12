package s3fs_test

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// compatScenarios are conformance checks modeled after the go-billy test
// suite. Each scenario runs against memfs (the reference implementation) and
// S3FS; both must exhibit the same behavior.
var compatScenarios = []struct {
	name string
	run  func(t *testing.T, bfs billy.Filesystem)
}{
	{"create write read", func(t *testing.T, bfs billy.Filesystem) {
		f, err := bfs.Create("foo/bar.txt")
		if err != nil {
			t.Fatal(err)
		}
		if f.Name() != "foo/bar.txt" && f.Name() != "/foo/bar.txt" {
			t.Fatalf("name = %q", f.Name())
		}
		if n, err := f.Write([]byte("hello")); err != nil || n != 5 {
			t.Fatalf("write: %d %v", n, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "foo/bar.txt"); string(got) != "hello" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"create truncates existing", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("long content"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.Create("f")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "f"); string(got) != "x" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"open missing", func(t *testing.T, bfs billy.Filesystem) {
		if _, err := bfs.Open("missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("open err = %v", err)
		}
		if _, err := bfs.Stat("missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat err = %v", err)
		}
	}},
	{"openfile excl", func(t *testing.T, bfs billy.Filesystem) {
		f, err := bfs.OpenFile("f", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		if _, err := bfs.OpenFile("f", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("err = %v, want ErrExist", err)
		}
	}},
	{"openfile append", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("abc"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.OpenFile("f", os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("def")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "f"); string(got) != "abcdef" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"openfile rdwr seek write", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("0123456789"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.OpenFile("f", os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Seek(4, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 2)
		if _, err := f.ReadAt(buf, 4); err != nil || string(buf) != "XY" {
			t.Fatalf("readat: %q %v", buf, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "f"); string(got) != "0123XY6789" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"writeat extends", func(t *testing.T, bfs billy.Filesystem) {
		f, err := bfs.Create("f")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("end"), 5); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "f"); string(got) != "\x00\x00\x00\x00\x00end" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"truncate shrink grow", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("0123456789"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.OpenFile("f", os.O_RDWR, 0o644)
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
		if got, _ := util.ReadFile(bfs, "f"); string(got) != "0123\x00\x00" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"seek current and end", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("0123456789"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.Open("f")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if pos, err := f.Seek(2, io.SeekStart); err != nil || pos != 2 {
			t.Fatalf("seek start: %d %v", pos, err)
		}
		if pos, err := f.Seek(3, io.SeekCurrent); err != nil || pos != 5 {
			t.Fatalf("seek current: %d %v", pos, err)
		}
		if pos, err := f.Seek(-1, io.SeekEnd); err != nil || pos != 9 {
			t.Fatalf("seek end: %d %v", pos, err)
		}
		b, err := io.ReadAll(f)
		if err != nil || string(b) != "9" {
			t.Fatalf("read: %q %v", b, err)
		}
	}},
	{"file stat", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := bfs.Open("f")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != 4 || fi.IsDir() {
			t.Fatalf("stat: size=%d dir=%v", fi.Size(), fi.IsDir())
		}
	}},
	{"rename file", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "a", []byte("1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bfs.Rename("a", "sub/b"); err != nil {
			t.Fatal(err)
		}
		if _, err := bfs.Stat("a"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("old still exists: %v", err)
		}
		if got, _ := util.ReadFile(bfs, "sub/b"); string(got) != "1" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"rename missing", func(t *testing.T, bfs billy.Filesystem) {
		if err := bfs.Rename("missing", "dst"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("err = %v", err)
		}
	}},
	{"rename replaces destination", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "src", []byte("new"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := util.WriteFile(bfs, "dst", []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bfs.Rename("src", "dst"); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "dst"); string(got) != "new" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"remove file", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bfs.Remove("f"); err != nil {
			t.Fatal(err)
		}
		if _, err := bfs.Stat("f"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("f should be gone")
		}
	}},
	{"remove missing", func(t *testing.T, bfs billy.Filesystem) {
		if err := bfs.Remove("missing"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("err = %v", err)
		}
	}},
	{"readdir sorted nested", func(t *testing.T, bfs billy.Filesystem) {
		for _, name := range []string{"b.txt", "a.txt", "sub/c.txt"} {
			if err := util.WriteFile(bfs, name, []byte("1"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		entries, err := bfs.ReadDir("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 3 {
			t.Fatalf("entries = %d", len(entries))
		}
		wantNames := []string{"a.txt", "b.txt", "sub"}
		wantDirs := []bool{false, false, true}
		for i, e := range entries {
			if e.Name() != wantNames[i] || e.IsDir() != wantDirs[i] {
				t.Fatalf("entry %d = %q dir=%v", i, e.Name(), e.IsDir())
			}
		}
		sub, err := bfs.ReadDir("sub")
		if err != nil || len(sub) != 1 || sub[0].Name() != "c.txt" {
			t.Fatalf("sub = %v %v", sub, err)
		}
	}},
	{"readdir entry info", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("12345"), 0o644); err != nil {
			t.Fatal(err)
		}
		entries, err := bfs.ReadDir("/")
		if err != nil || len(entries) != 1 {
			t.Fatalf("entries: %v %v", entries, err)
		}
		fi, err := entries[0].Info()
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != 5 || fi.Name() != "f" {
			t.Fatalf("info: %q %d", fi.Name(), fi.Size())
		}
	}},
	{"mkdirall idempotent", func(t *testing.T, bfs billy.Filesystem) {
		if err := bfs.MkdirAll("x/y/z", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := bfs.MkdirAll("x/y/z", 0o755); err != nil {
			t.Fatal(err)
		}
		for _, dir := range []string{"x", "x/y", "x/y/z"} {
			fi, err := bfs.Stat(dir)
			if err != nil || !fi.IsDir() {
				t.Fatalf("stat %s: %v %v", dir, fi, err)
			}
		}
	}},
	{"mkdirall over file", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bfs.MkdirAll("f", 0o755); err == nil {
			t.Fatal("expected error")
		}
	}},
	{"symlink roundtrip", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "target", []byte("real"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bfs.Symlink("target", "link"); err != nil {
			t.Fatal(err)
		}
		if got, err := bfs.Readlink("link"); err != nil || got != "target" {
			t.Fatalf("readlink: %q %v", got, err)
		}
		fi, err := bfs.Lstat("link")
		if err != nil || fi.Mode()&fs.ModeSymlink == 0 {
			t.Fatalf("lstat: %v %v", fi, err)
		}
		fi, err = bfs.Stat("link")
		if err != nil || fi.Mode()&fs.ModeSymlink != 0 {
			t.Fatalf("stat should follow: %v %v", fi, err)
		}
		if got, _ := util.ReadFile(bfs, "link"); string(got) != "real" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"symlink dangling", func(t *testing.T, bfs billy.Filesystem) {
		if err := bfs.Symlink("missing-target", "link"); err != nil {
			t.Fatal(err)
		}
		if _, err := bfs.Lstat("link"); err != nil {
			t.Fatalf("lstat: %v", err)
		}
		if _, err := bfs.Stat("link"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stat: %v", err)
		}
		if _, err := bfs.Open("link"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("open: %v", err)
		}
	}},
	{"readlink on regular file", func(t *testing.T, bfs billy.Filesystem) {
		if err := util.WriteFile(bfs, "f", []byte("1"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := bfs.Readlink("f"); err == nil {
			t.Fatal("expected error")
		}
	}},
	{"tempfile unique", func(t *testing.T, bfs billy.Filesystem) {
		f1, err := bfs.TempFile("tmp", "p-")
		if err != nil {
			t.Fatal(err)
		}
		f2, err := bfs.TempFile("tmp", "p-")
		if err != nil {
			t.Fatal(err)
		}
		if f1.Name() == f2.Name() {
			t.Fatalf("collision: %s", f1.Name())
		}
		if _, err := f1.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := f1.Close(); err != nil {
			t.Fatal(err)
		}
		f2.Close()
		if got, _ := util.ReadFile(bfs, f1.Name()); string(got) != "x" {
			t.Fatalf("content = %q", got)
		}
	}},
	{"chroot isolated", func(t *testing.T, bfs billy.Filesystem) {
		sub, err := bfs.Chroot("jail")
		if err != nil {
			t.Fatal(err)
		}
		if err := util.WriteFile(sub, "f", []byte("inside"), 0o644); err != nil {
			t.Fatal(err)
		}
		if got, _ := util.ReadFile(bfs, "jail/f"); string(got) != "inside" {
			t.Fatalf("outer view = %q", got)
		}
		if _, err := sub.Open("../escape"); err == nil {
			t.Fatal("chroot escape should fail")
		}
	}},
	{"removeall tree", func(t *testing.T, bfs billy.Filesystem) {
		for _, name := range []string{"d/a", "d/sub/b", "d/sub/deep/c"} {
			if err := util.WriteFile(bfs, name, []byte("1"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := util.RemoveAll(bfs, "d"); err != nil {
			t.Fatal(err)
		}
		if _, err := bfs.Stat("d"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("d should be gone: %v", err)
		}
	}},
	{"capabilities", func(t *testing.T, bfs billy.Filesystem) {
		need := billy.ReadCapability | billy.WriteCapability |
			billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
		if !billy.CapabilityCheck(bfs, need) {
			t.Fatalf("caps = %b", billy.Capabilities(bfs))
		}
	}},
}

// TestBillyCompat runs the same conformance scenarios against memfs and S3FS.
func TestBillyCompat(t *testing.T) {
	impls := []struct {
		name string
		make func(t *testing.T) billy.Filesystem
	}{
		{"memfs", func(t *testing.T) billy.Filesystem { return memfs.New() }},
		{"s3fs", func(t *testing.T) billy.Filesystem { return newTestFS(t) }},
		{"s3fs-cached", func(t *testing.T) billy.Filesystem { return newTestFS(t, s3fs.WithCache(64<<20, 0)) }},
	}
	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			for _, sc := range compatScenarios {
				t.Run(sc.name, func(t *testing.T) {
					sc.run(t, impl.make(t))
				})
			}
		})
	}
}
