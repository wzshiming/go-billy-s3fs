# go-billy-s3fs

A [go-billy](https://github.com/go-git/go-billy) filesystem backed by Amazon S3 (or any S3-compatible object store), usable as storage for [go-git](https://github.com/go-git/go-git).

## Scope: large files are out of scope

This filesystem is designed for git storage, where objects and packfiles are expected to be small. Git itself is not suited to versioning large files — in practice those are stored with [Git LFS](https://git-lfs.com) (or similar) rather than in the repository, so this library deliberately does not optimize for them:

- Writes are buffered entirely in memory and uploaded with a single `PutObject` on `Close`/`Sync`, which caps a single object at S3's 5 GB `PutObject` limit.
- `Rename` uses a single server-side `CopyObject`, which is also capped at 5 GB per object.

Multipart upload/copy paths for objects beyond these limits are intentionally not planned. If you need to store files that large, keep them out of git (e.g. via Git LFS) instead of pushing them through this filesystem.

## File locking

File handles implement `billy.Locker` with flock-like semantics: `Lock` takes an exclusive, blocking, advisory lock on the file's path; a second `Lock` by the same handle is a no-op; `Unlock` without a held lock is a no-op; `Close` releases any held lock.

By default locks only exclude handles of the same `S3FS` instance. To make them mutual across processes, configure a cross-process backend:

```go
client := s3.NewFromConfig(cfg)
fs := s3fs.New(client, "bucket",
	s3fs.WithPrefix("repo"),
	s3fs.WithLocker(s3fs.NewS3Locker(client, "bucket")),
)
```

`S3Locker` stores each lock as an object (default prefix `.s3fs-lock/`, keep it outside the data prefix) created with a conditional `If-None-Match: "*"` write, so only one client can hold it. Holders renew a TTL lease in the background (`WithLockTTL`, default 60s); locks left by a crashed process are taken over once the lease expires, with `If-Match` guarding against concurrent takeovers. Blocked `Lock`s poll (`WithLockPoll`, default 1s) and abort when the filesystem context (`WithContext`) is cancelled.

Caveats: the store must support conditional writes (AWS S3, MinIO and gofakes3 do), client clocks must agree well within the TTL, and like every lease-based lock it is advisory — a holder that cannot renew (e.g. a network outage longer than the TTL) silently loses the lock.

Any other coordination service can be plugged in by implementing the two-method `Locker` interface (e.g. Redis with SET NX PX): S3FS serializes lock holders per path within the process, so an implementation only arbitrates between processes and needs no reentrancy.
