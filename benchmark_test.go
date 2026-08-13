package s3fs_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// benchVariants are the filesystems the benchmarks compare. The s3fs variants
// talk to an in-memory fake S3 server over localhost HTTP, so results reflect
// protocol and buffering overhead rather than real network latency.
var benchVariants = []struct {
	name string
	make func(b *testing.B) billy.Filesystem
}{
	{"memfs", func(b *testing.B) billy.Filesystem { return memfs.New() }},
	{"osfs", func(b *testing.B) billy.Filesystem { return osfs.New(b.TempDir()) }},
	{"s3fs", func(b *testing.B) billy.Filesystem {
		return s3fs.New(testBucket, s3fs.WithClient(newTestClient(b)))
	}},
	{"s3fs-cached", func(b *testing.B) billy.Filesystem {
		return s3fs.New(testBucket, s3fs.WithClient(newTestClient(b)), s3fs.WithMemCache(64<<20, 0))
	}},
	{"s3fs-disk", func(b *testing.B) billy.Filesystem {
		return newTestFS(b, s3fs.WithDiskCache(b.TempDir(), 256<<20, 0))
	}},
	{"s3fs-disk-spill", func(b *testing.B) billy.Filesystem {
		// tiny memory cache forces every body through the disk tier and
		// every write through a spill file
		return newTestFS(b, s3fs.WithMemCache(4<<10, 0), s3fs.WithDiskCache(b.TempDir(), 256<<20, 0))
	}},
}

var benchSizes = []struct {
	name string
	n    int
}{
	{"4KiB", 4 << 10},
	{"1MiB", 1 << 20},
}

func BenchmarkWrite(b *testing.B) {
	for _, size := range benchSizes {
		payload := bytes.Repeat([]byte("x"), size.n)
		b.Run(size.name, func(b *testing.B) {
			for _, v := range benchVariants {
				b.Run(v.name, func(b *testing.B) {
					bfs := v.make(b)
					b.ReportAllocs()
					b.SetBytes(int64(size.n))
					for b.Loop() {
						f, err := bfs.Create("bench.dat")
						if err != nil {
							b.Fatal(err)
						}
						if _, err := f.Write(payload); err != nil {
							b.Fatal(err)
						}
						if err := f.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	for _, size := range benchSizes {
		payload := bytes.Repeat([]byte("x"), size.n)
		b.Run(size.name, func(b *testing.B) {
			for _, v := range benchVariants {
				b.Run(v.name, func(b *testing.B) {
					bfs := v.make(b)
					// Written through the same fs, so the cached variant reads hits.
					if err := util.WriteFile(bfs, "bench.dat", payload, 0o644); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.SetBytes(int64(size.n))
					for b.Loop() {
						f, err := bfs.Open("bench.dat")
						if err != nil {
							b.Fatal(err)
						}
						n, err := io.Copy(io.Discard, f)
						if err != nil {
							b.Fatal(err)
						}
						if n != int64(size.n) {
							b.Fatalf("read %d bytes, want %d", n, size.n)
						}
						if err := f.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkStat(b *testing.B) {
	for _, v := range benchVariants {
		b.Run(v.name, func(b *testing.B) {
			bfs := v.make(b)
			if err := util.WriteFile(bfs, "bench.dat", []byte("x"), 0o644); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := bfs.Stat("bench.dat"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadDir(b *testing.B) {
	const numFiles = 128
	for _, v := range benchVariants {
		b.Run(v.name, func(b *testing.B) {
			bfs := v.make(b)
			for i := range numFiles {
				name := fmt.Sprintf("dir/f%03d", i)
				if err := util.WriteFile(bfs, name, []byte("x"), 0o644); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				entries, err := bfs.ReadDir("dir")
				if err != nil {
					b.Fatal(err)
				}
				if len(entries) != numFiles {
					b.Fatalf("ReadDir returned %d entries, want %d", len(entries), numFiles)
				}
			}
		})
	}
}
