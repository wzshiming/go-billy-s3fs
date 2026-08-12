package s3fs_test

import (
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

var testSignature = &object.Signature{
	Name:  "s3fs test",
	Email: "s3fs@example.com",
	When:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
}

// TestGoGitEndToEnd stores a whole git repository in S3, reopens it from a
// fresh filesystem instance and verifies history, then repacks.
func TestGoGitEndToEnd(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) { testGoGitEndToEnd(t, v.opts) })
	}
}

func testGoGitEndToEnd(t *testing.T, opts []s3fs.Option) {
	client := newTestClient(t)
	newFS := func() *s3fs.S3FS {
		return s3fs.New(client, testBucket, append([]s3fs.Option{s3fs.WithPrefix("repos/demo.git")}, opts...)...)
	}
	bfs := newFS()

	// init a repository whose object database lives in S3
	st := filesystem.NewStorage(bfs, cache.NewObjectLRUDefault())
	wt := memfs.New()
	repo, err := git.Init(st, git.WithWorkTree(wt))
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	w, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}

	var lastHash string
	for i, step := range []struct{ file, content, msg string }{
		{"README.md", "# demo\n", "initial commit"},
		{"main.go", "package main\n", "add main"},
		{"README.md", "# demo v2\n", "update readme"},
	} {
		if err := util.WriteFile(wt, step.file, []byte(step.content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Add(step.file); err != nil {
			t.Fatalf("add step %d: %v", i, err)
		}
		h, err := w.Commit(step.msg, &git.CommitOptions{Author: testSignature, Committer: testSignature})
		if err != nil {
			t.Fatalf("commit step %d: %v", i, err)
		}
		lastHash = h.String()
	}

	// reopen the repository from scratch, backed by the same bucket
	bfs2 := newFS()
	st2 := filesystem.NewStorage(bfs2, cache.NewObjectLRUDefault())
	repo2, err := git.Open(st2, memfs.New())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	head, err := repo2.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Hash().String() != lastHash {
		t.Fatalf("head = %s, want %s", head.Hash(), lastHash)
	}

	commit, err := repo2.CommitObject(head.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if commit.Message != "update readme" {
		t.Fatalf("message = %q", commit.Message)
	}

	// full history walk exercises loose object reads
	iter, err := repo2.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := iter.ForEach(func(*object.Commit) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("commits = %d, want 3", count)
	}

	// verify file content of HEAD tree
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	f, err := tree.File("README.md")
	if err != nil {
		t.Fatal(err)
	}
	content, err := f.Contents()
	if err != nil {
		t.Fatal(err)
	}
	if content != "# demo v2\n" {
		t.Fatalf("README = %q", content)
	}

	// repack: writes a packfile via TempFile+Rename and reads it back via ReadAt
	if err := repo2.RepackObjects(&git.RepackConfig{}); err != nil {
		t.Fatalf("repack: %v", err)
	}

	bfs3 := newFS()
	st3 := filesystem.NewStorage(bfs3, cache.NewObjectLRUDefault())
	repo3, err := git.Open(st3, memfs.New())
	if err != nil {
		t.Fatalf("open after repack: %v", err)
	}
	commit3, err := repo3.CommitObject(head.Hash())
	if err != nil {
		t.Fatalf("read commit from pack: %v", err)
	}
	if commit3.Message != "update readme" {
		t.Fatalf("message after repack = %q", commit3.Message)
	}
	iter3, err := repo3.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	if err := iter3.ForEach(func(*object.Commit) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("commits after repack = %d, want 3", count)
	}
}

// TestGoGitBare verifies a bare repository can be initialized and reopened.
func TestGoGitBare(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) { testGoGitBare(t, v.opts) })
	}
}

func testGoGitBare(t *testing.T, opts []s3fs.Option) {
	client := newTestClient(t)
	newFS := func() *s3fs.S3FS {
		return s3fs.New(client, testBucket, append([]s3fs.Option{s3fs.WithPrefix("bare.git")}, opts...)...)
	}
	bfs := newFS()
	st := filesystem.NewStorage(bfs, cache.NewObjectLRUDefault())
	if _, err := git.Init(st); err != nil {
		t.Fatalf("init bare: %v", err)
	}

	bfs2 := newFS()
	st2 := filesystem.NewStorage(bfs2, cache.NewObjectLRUDefault())
	repo, err := git.Open(st2, nil)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	cfg, err := repo.Config()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Core.IsBare {
		t.Fatal("expected bare repository")
	}
}
