package s3fs_test

import (
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// TestGoGitPushCloneE2E exercises the full git wire protocol against a bare
// repository stored in S3: push over receive-pack, then clone twice over
// upload-pack, verifying content round-trips.
func TestGoGitPushCloneE2E(t *testing.T) {
	s3Client := newTestClient(t)

	// bare remote repository living in S3
	remoteFS := s3fs.New(s3Client, testBucket, s3fs.WithPrefix("remote.git"))
	remoteStore := filesystem.NewStorage(remoteFS, cache.NewObjectLRUDefault())
	if _, err := git.Init(remoteStore); err != nil {
		t.Fatalf("init remote: %v", err)
	}

	// route file:///remote.git to the S3-backed storer, in process
	loader := transport.MapLoader{"/remote.git": remoteStore}
	clientOpts := []client.Option{client.WithLoader(loader)}
	remoteURL := "file:///remote.git"

	// local repository: commit and push to S3 via receive-pack
	local, err := git.Init(memory.NewStorage(), git.WithWorkTree(memfs.New()))
	if err != nil {
		t.Fatal(err)
	}
	w, err := local.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	wt := w.Filesystem()

	var hashes []plumbing.Hash
	for _, step := range []struct{ file, content, msg string }{
		{"README.md", "# e2e\n", "initial commit"},
		{"app/main.go", "package main\n", "add app"},
	} {
		if err := util.WriteFile(wt, step.file, []byte(step.content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Add(step.file); err != nil {
			t.Fatal(err)
		}
		h, err := w.Commit(step.msg, &git.CommitOptions{Author: testSignature, Committer: testSignature})
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, h)
	}

	if _, err := local.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{remoteURL},
	}); err != nil {
		t.Fatal(err)
	}
	if err := local.Push(&git.PushOptions{
		RemoteName:    "origin",
		RefSpecs:      []config.RefSpec{"refs/heads/*:refs/heads/*"},
		ClientOptions: clientOpts,
	}); err != nil {
		t.Fatalf("push: %v", err)
	}

	// remote HEAD symref must be resolvable for clones
	headRef, err := remoteStore.Reference(plumbing.HEAD)
	if err != nil {
		t.Fatalf("remote HEAD: %v", err)
	}
	branch := headRef.Target()
	if _, err := remoteStore.Reference(branch); err != nil {
		t.Fatalf("remote branch %s: %v", branch, err)
	}

	// clone from S3 into memory via upload-pack
	clone1, err := git.Clone(memory.NewStorage(), memfs.New(), &git.CloneOptions{
		URL:           remoteURL,
		ClientOptions: clientOpts,
	})
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	verifyClone(t, clone1, hashes[len(hashes)-1], len(hashes))

	// second round: push an update, then fetch and re-clone
	if err := util.WriteFile(wt, "README.md", []byte("# e2e v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Add("README.md"); err != nil {
		t.Fatal(err)
	}
	h3, err := w.Commit("update readme", &git.CommitOptions{Author: testSignature, Committer: testSignature})
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Push(&git.PushOptions{
		RemoteName:    "origin",
		RefSpecs:      []config.RefSpec{"refs/heads/*:refs/heads/*"},
		ClientOptions: clientOpts,
	}); err != nil {
		t.Fatalf("second push: %v", err)
	}

	// fetch the update into the first clone
	if err := clone1.Fetch(&git.FetchOptions{
		RemoteName:    "origin",
		ClientOptions: clientOpts,
	}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if _, err := clone1.CommitObject(h3); err != nil {
		t.Fatalf("fetched commit: %v", err)
	}

	// a fresh clone sees all three commits, with worktree checked out
	cloneFS := memfs.New()
	clone2, err := git.Clone(memory.NewStorage(), cloneFS, &git.CloneOptions{
		URL:           remoteURL,
		ClientOptions: clientOpts,
	})
	if err != nil {
		t.Fatalf("clone after update: %v", err)
	}
	verifyClone(t, clone2, h3, 3)
	if got, err := util.ReadFile(cloneFS, "README.md"); err != nil || string(got) != "# e2e v2\n" {
		t.Fatalf("worktree README = %q, %v", got, err)
	}
	if got, err := util.ReadFile(cloneFS, "app/main.go"); err != nil || string(got) != "package main\n" {
		t.Fatalf("worktree main.go = %q, %v", got, err)
	}
}

// TestGoGitCloneIntoS3E2E uses S3 as the *client* side: clone a memory-backed
// remote into S3 storage, exercising packfile writes through the transport.
func TestGoGitCloneIntoS3E2E(t *testing.T) {
	// source repository in memory
	srcStore := memory.NewStorage()
	src, err := git.Init(srcStore, git.WithWorkTree(memfs.New()))
	if err != nil {
		t.Fatal(err)
	}
	w, err := src.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if err := util.WriteFile(w.Filesystem(), "data.txt", []byte("payload\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Add("data.txt"); err != nil {
		t.Fatal(err)
	}
	want, err := w.Commit("source commit", &git.CommitOptions{Author: testSignature, Committer: testSignature})
	if err != nil {
		t.Fatal(err)
	}

	loader := transport.MapLoader{"/src.git": srcStore}
	clientOpts := []client.Option{client.WithLoader(loader)}

	// destination storage in S3
	s3Client := newTestClient(t)
	dstFS := s3fs.New(s3Client, testBucket, s3fs.WithPrefix("mirror.git"))
	dstStore := filesystem.NewStorage(dstFS, cache.NewObjectLRUDefault())
	repo, err := git.Clone(dstStore, memfs.New(), &git.CloneOptions{
		URL:           "file:///src.git",
		ClientOptions: clientOpts,
	})
	if err != nil {
		t.Fatalf("clone into s3: %v", err)
	}
	if _, err := repo.CommitObject(want); err != nil {
		t.Fatalf("commit in s3 clone: %v", err)
	}

	// reopen from a fresh S3FS instance and verify the packfile is readable
	dstFS2 := s3fs.New(s3Client, testBucket, s3fs.WithPrefix("mirror.git"))
	dstStore2 := filesystem.NewStorage(dstFS2, cache.NewObjectLRUDefault())
	reopened, err := git.Open(dstStore2, memfs.New())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	commit, err := reopened.CommitObject(want)
	if err != nil {
		t.Fatalf("commit after reopen: %v", err)
	}
	if commit.Message != "source commit" {
		t.Fatalf("message = %q", commit.Message)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	f, err := tree.File("data.txt")
	if err != nil {
		t.Fatal(err)
	}
	if content, _ := f.Contents(); content != "payload\n" {
		t.Fatalf("content = %q", content)
	}
}

func verifyClone(t *testing.T, repo *git.Repository, wantHead plumbing.Hash, wantCommits int) {
	t.Helper()
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Hash() != wantHead {
		t.Fatalf("head = %s, want %s", head.Hash(), wantHead)
	}
	iter, err := repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := iter.ForEach(func(*object.Commit) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != wantCommits {
		t.Fatalf("commits = %d, want %d", count, wantCommits)
	}
}
