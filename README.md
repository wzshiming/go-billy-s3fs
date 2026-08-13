# go-billy-s3fs

A [go-billy](https://github.com/go-git/go-billy) filesystem backed by Amazon S3 (or any S3-compatible object store), usable as storage for [go-git](https://github.com/go-git/go-git).

## Scope: large files are out of scope

This filesystem is designed for git storage, where objects and packfiles are expected to be small. Git itself is not suited to versioning large files — in practice those are stored with [Git LFS](https://git-lfs.com) (or similar) rather than in the repository, so this library deliberately does not optimize for them:

- Writes are buffered entirely in memory and uploaded with a single `PutObject` on `Close`/`Sync`, which caps a single object at S3's 5 GB `PutObject` limit.
- `Rename` uses a single server-side `CopyObject`, which is also capped at 5 GB per object.

Multipart upload/copy paths for objects beyond these limits are intentionally not planned. If you need to store files that large, keep them out of git (e.g. via Git LFS) instead of pushing them through this filesystem.
