package s3fs

import (
	"io/fs"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

var _ fs.FileInfo = (*fileInfo)(nil)

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *fileInfo) Sys() any           { return nil }

type dirEntry struct {
	info fileInfo
}

var _ fs.DirEntry = (*dirEntry)(nil)

func (d *dirEntry) Name() string               { return d.info.name }
func (d *dirEntry) IsDir() bool                { return d.info.IsDir() }
func (d *dirEntry) Type() fs.FileMode          { return d.info.mode.Type() }
func (d *dirEntry) Info() (fs.FileInfo, error) { return &d.info, nil }

// modeFromHead returns the permission bits persisted in object metadata,
// falling back to the default file mode.
func modeFromHead(h *s3.HeadObjectOutput) fs.FileMode {
	if v, ok := metaValue(h.Metadata, modeMetaKey); ok {
		if parsed, err := strconv.ParseUint(v, 8, 32); err == nil {
			return fs.FileMode(parsed).Perm()
		}
	}
	return defaultFileMode
}

// infoFromHeadValue builds a fileInfo from a HeadObject response, restoring
// the permission bits persisted in object metadata when present.
func infoFromHeadValue(name string, h *s3.HeadObjectOutput) fileInfo {
	return fileInfo{
		name:    name,
		size:    aws.ToInt64(h.ContentLength),
		mode:    modeFromHead(h),
		modTime: aws.ToTime(h.LastModified),
	}
}

func infoFromHead(name string, h *s3.HeadObjectOutput) *fileInfo {
	info := infoFromHeadValue(name, h)
	return &info
}
