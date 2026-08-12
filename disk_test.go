package s3fs_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// tinyMemOpts makes the memory cache hold at most 1KB bodies, so 4KB test
// payloads are forced through the disk tier and write spill.
func tinyMemOpts(dir string) []s3fs.Option {
	return []s3fs.Option{s3fs.WithMemCache(8<<10, 0), s3fs.WithDiskCache(dir, 1<<20, 0)}
}

func writeBytes(bfs billy.Filesystem, name string, data []byte) error {
	return util.WriteFile(bfs, name, data, 0o644)
}

func dirFiles(t *testing.T, dir, prefix string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, prefix))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

// TestDiskCacheServesSecondOpenLocally verifies that a body too large for
// the memory cache is downloaded once and then served from local disk.
func TestDiskCacheServesSecondOpenLocally(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	dir := t.TempDir()
	bfs := s3fs.New(client, testBucket, s3fs.WithDiskCache(dir, 1<<20, 0))
	content := strings.Repeat("packfile-bytes.", 300) // ~4.5KB

	writeFull(t, bfs, "objects/pack/pack-1.pack", content)

	if got := readFull(t, bfs, "objects/pack/pack-1.pack"); got != content {
		t.Fatalf("content mismatch: %d bytes", len(got))
	}
	gets := client.count("GetObject")

	// second open: body must come from disk, no GetObject
	f, err := bfs.Open("objects/pack/pack-1.pack")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if _, err := f.ReadAt(buf, 1024); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(buf), content[1024:1024+8]) {
		t.Fatalf("ReadAt content mismatch: %q", buf[:8])
	}
	rest, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != content {
		t.Fatalf("sequential read mismatch: %d bytes", len(rest))
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if n := client.count("GetObject") - gets; n != 0 {
		t.Fatalf("GetObject on second open = %d, want 0", n)
	}
}

// TestDiskSpillWriteAdoptedIntoCache verifies large writes buffer on disk,
// upload once, and are then readable with no download at all.
func TestDiskSpillWriteAdoptedIntoCache(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	dir := t.TempDir()
	bfs := s3fs.New(client, testBucket, tinyMemOpts(dir)...)
	content := bytes.Repeat([]byte("spill-me!"), 1000) // 9KB > 1KB threshold

	f, err := bfs.Create("big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	// buffer must be a spill file, not memory
	if n := len(dirFiles(t, dir, "s3fs-w-*")); n != 1 {
		t.Fatalf("spill files while open = %d, want 1", n)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if n := client.count("PutObject"); n != 1 {
		t.Fatalf("PutObject = %d, want 1", n)
	}
	// spill buffer released, body adopted into the cache
	if n := len(dirFiles(t, dir, "s3fs-w-*")); n != 0 {
		t.Fatalf("spill files after close = %d, want 0", n)
	}
	if n := len(dirFiles(t, dir, "s3fs-c-*")); n != 1 {
		t.Fatalf("cache files after close = %d, want 1", n)
	}

	gets := client.count("GetObject")
	if got := readFull(t, bfs, "big.bin"); got != string(content) {
		t.Fatalf("content mismatch: %d bytes", len(got))
	}
	if n := client.count("GetObject") - gets; n != 0 {
		t.Fatalf("GetObject after adopted write = %d, want 0", n)
	}
}

// TestDiskCacheSurvivesRename verifies the go-git packfile flow: write a
// temp file (spilled), rename it into place, then read it back without any
// download thanks to the hard-linked cache entry.
func TestDiskCacheSurvivesRename(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	dir := t.TempDir()
	bfs := s3fs.New(client, testBucket, tinyMemOpts(dir)...)
	content := bytes.Repeat([]byte("rename-me"), 1000)

	if err := writeBytes(bfs, "tmp_pack", content); err != nil {
		t.Fatal(err)
	}
	if err := bfs.Rename("tmp_pack", "objects/pack/pack-final.pack"); err != nil {
		t.Fatal(err)
	}

	gets := client.count("GetObject")
	if got := readFull(t, bfs, "objects/pack/pack-final.pack"); got != string(content) {
		t.Fatalf("content mismatch: %d bytes", len(got))
	}
	if n := client.count("GetObject") - gets; n != 0 {
		t.Fatalf("GetObject after rename = %d, want 0", n)
	}
}

// TestDiskCacheInvalidatedByOverwriteAndDelete verifies stale bodies are
// never served after the object changes or disappears.
func TestDiskCacheInvalidatedByOverwriteAndDelete(t *testing.T) {
	dir := t.TempDir()
	bfs := newTestFS(t, s3fs.WithDiskCache(dir, 1<<20, 0))
	v1 := strings.Repeat("one", 1500)
	v2 := strings.Repeat("two", 1500)

	writeFull(t, bfs, "f.bin", v1)
	if got := readFull(t, bfs, "f.bin"); got != v1 {
		t.Fatal("v1 mismatch")
	}
	writeFull(t, bfs, "f.bin", v2)
	if got := readFull(t, bfs, "f.bin"); got != v2 {
		t.Fatal("read after overwrite returned stale body")
	}
	if err := bfs.Remove("f.bin"); err != nil {
		t.Fatal(err)
	}
	if _, err := bfs.Open("f.bin"); err == nil {
		t.Fatal("open after delete succeeded")
	}
	if n := len(dirFiles(t, dir, "s3fs-c-*")); n != 0 {
		t.Fatalf("cache files after delete = %d, want 0", n)
	}
}

// TestDiskCacheEviction verifies the LRU keeps total disk usage under
// budget by dropping the oldest entry, which is then re-fetched on demand.
func TestDiskCacheEviction(t *testing.T) {
	client := newCountingClient(newTestClient(t))
	dir := t.TempDir()
	// budget fits one ~4KB body (+overhead) but not two
	bfs := s3fs.New(client, testBucket, s3fs.WithDiskCache(dir, 6<<10, 0))
	blob := strings.Repeat("x", 4<<10)

	writeFull(t, bfs, "a.bin", blob)
	writeFull(t, bfs, "b.bin", blob)
	readFull(t, bfs, "a.bin") // caches a
	readFull(t, bfs, "b.bin") // evicts a
	if n := len(dirFiles(t, dir, "s3fs-c-*")); n != 1 {
		t.Fatalf("cache files = %d, want 1", n)
	}

	gets := client.count("GetObject")
	readFull(t, bfs, "b.bin") // hit
	if n := client.count("GetObject") - gets; n != 0 {
		t.Fatalf("GetObject for cached b = %d, want 0", n)
	}
	readFull(t, bfs, "a.bin") // miss, re-fetch
	if n := client.count("GetObject") - gets; n != 1 {
		t.Fatalf("GetObject for evicted a = %d, want 1", n)
	}
}

// TestDiskSpillSharedReader verifies a read handle opened while a spilled
// write is in progress observes the live buffer, and the spill file is
// removed only after the last handle closes.
func TestDiskSpillSharedReader(t *testing.T) {
	dir := t.TempDir()
	bfs := newTestFS(t, tinyMemOpts(dir)...)
	content := bytes.Repeat([]byte("live"), 2000) // 8KB

	w, err := bfs.Create("pack.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
	r, err := bfs.Open("pack.tmp")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("shared read = %d bytes, want %d", len(got), len(content))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// reader still open: spill must survive
	if _, err := r.ReadAt(make([]byte, 4), 0); err != nil {
		t.Fatalf("read after writer close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(dirFiles(t, dir, "s3fs-w-*")); n != 0 {
		t.Fatalf("spill files after all closes = %d, want 0", n)
	}
}

// TestDiskReadModifyWriteLargeObject verifies opening an existing large
// object O_RDWR streams it into a spill buffer and round-trips edits.
func TestDiskReadModifyWriteLargeObject(t *testing.T) {
	dir := t.TempDir()
	bfs := newTestFS(t, tinyMemOpts(dir)...)
	content := bytes.Repeat([]byte("0123456789"), 1000) // 10KB

	if err := writeBytes(bfs, "data.bin", content); err != nil {
		t.Fatal(err)
	}
	f, err := bfs.OpenFile("data.bin", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("EDIT"), 5000); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	want := append([]byte{}, content...)
	copy(want[5000:], "EDIT")
	if got := readFull(t, bfs, "data.bin"); got != string(want) {
		t.Fatal("read-modify-write mismatch")
	}
}

// TestDiskCacheLeftoverSweep verifies files from a previous process are
// removed once the cache is first used.
func TestDiskCacheLeftoverSweep(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "s3fs-c-999")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	bfs := newTestFS(t, s3fs.WithDiskCache(dir, 1<<20, 0))
	writeFull(t, bfs, "x.bin", strings.Repeat("y", 4<<10))
	readFull(t, bfs, "x.bin") // first disk use triggers the sweep
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file still present: %v", err)
	}
}
