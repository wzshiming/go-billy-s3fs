package s3fs_test

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	s3fs "github.com/wzshiming/go-billy-s3fs"
)

// doSigned performs the presigned request with the given body; the URL is
// self-contained, so no extra headers are set.
func doSigned(t *testing.T, method, url string, body io.Reader) *http.Response {
	t.Helper()
	httpReq, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestPresignGet(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) {
			bfs := newTestFS(t, v.opts(t)...)
			writeFull(t, bfs, "dir/data.txt", "signed content")

			req, err := bfs.PresignGet("dir/data.txt", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if req.Method != http.MethodGet {
				t.Fatalf("method = %q, want GET", req.Method)
			}
			resp := doSigned(t, req.Method, req.URL, nil)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "signed content" {
				t.Fatalf("body = %q", data)
			}
		})
	}
}

func TestPresignGetFollowsSymlink(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "target.txt", "via link")
	if err := bfs.Symlink("target.txt", "link.txt"); err != nil {
		t.Fatal(err)
	}

	req, err := bfs.PresignGet("link.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(req.URL, "target.txt") {
		t.Fatalf("URL %q does not point at the symlink target", req.URL)
	}
	resp := doSigned(t, req.Method, req.URL, nil)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "via link" {
		t.Fatalf("body = %q", data)
	}
}

func TestPresignGetErrors(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "dir/data.txt", "x")

	if _, err := bfs.PresignGet("missing.txt", 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing file: err = %v, want fs.ErrNotExist", err)
	}
	if _, err := bfs.PresignGet("dir", 0); !errors.Is(err, s3fs.ErrIsDirectory) {
		t.Fatalf("directory: err = %v, want ErrIsDirectory", err)
	}
	if _, err := bfs.PresignGet("/", 0); !errors.Is(err, s3fs.ErrIsDirectory) {
		t.Fatalf("root: err = %v, want ErrIsDirectory", err)
	}
}

func TestPresignPut(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) {
			bfs := newTestFS(t, v.opts(t)...)

			// prime a negative cache entry that signing must forget
			if _, err := bfs.Stat("up/new.txt"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("stat before upload: %v", err)
			}

			req, err := bfs.PresignPut("up/new.txt", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if req.Method != http.MethodPut {
				t.Fatalf("method = %q, want PUT", req.Method)
			}
			resp := doSigned(t, req.Method, req.URL, strings.NewReader("uploaded body"))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}

			if got := readFull(t, bfs, "up/new.txt"); got != "uploaded body" {
				t.Fatalf("content = %q, want %q", got, "uploaded body")
			}
		})
	}
}

func TestPresignPutOverwrite(t *testing.T) {
	for _, v := range fsVariants {
		t.Run(v.name, func(t *testing.T) {
			bfs := newTestFS(t, v.opts(t)...)
			writeFull(t, bfs, "file.txt", "old")
			if got := readFull(t, bfs, "file.txt"); got != "old" {
				t.Fatalf("content = %q", got)
			}

			req, err := bfs.PresignPut("file.txt", 0)
			if err != nil {
				t.Fatal(err)
			}
			resp := doSigned(t, req.Method, req.URL, strings.NewReader("new"))
			resp.Body.Close()

			if got := readFull(t, bfs, "file.txt"); got != "new" {
				t.Fatalf("content = %q, want %q", got, "new")
			}
		})
	}
}

func TestPresignPutErrors(t *testing.T) {
	bfs := newTestFS(t)
	if err := bfs.MkdirAll("dir", 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := bfs.PresignPut("dir", 0); !errors.Is(err, s3fs.ErrIsDirectory) {
		t.Fatalf("directory: err = %v, want ErrIsDirectory", err)
	}
	if _, err := bfs.PresignPut("/", 0); !errors.Is(err, s3fs.ErrIsDirectory) {
		t.Fatalf("root: err = %v, want ErrIsDirectory", err)
	}
}

// TestPresignerAssert detects presign support behind a billy.Filesystem, the
// intended use of the Presigner interface.
func TestPresignerAssert(t *testing.T) {
	var bfs billy.Filesystem = newTestFS(t)
	writeFull(t, bfs, "a.txt", "content")

	p, ok := bfs.(s3fs.Presigner)
	if !ok {
		t.Fatal("*S3FS does not satisfy s3fs.Presigner")
	}
	req, err := p.PresignGet("a.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if req.URL == "" {
		t.Fatal("empty presigned URL")
	}
}

// TestPresignNoClient verifies presigning is an opt-in dependency.
func TestPresignNoClient(t *testing.T) {
	bfs := s3fs.New(testBucket, s3fs.WithClient(newTestClient(t)))
	if _, err := bfs.PresignGet("a.txt", 0); !errors.Is(err, s3fs.ErrNoPresignClient) {
		t.Fatalf("PresignGet err = %v, want ErrNoPresignClient", err)
	}
	if _, err := bfs.PresignPut("a.txt", 0); !errors.Is(err, s3fs.ErrNoPresignClient) {
		t.Fatalf("PresignPut err = %v, want ErrNoPresignClient", err)
	}
}

// TestPresignPureURL verifies the URL is self-contained even for a client
// with the SDK's default checksum behavior, which presigning must override:
// only the host header may be signed, so a bare request needs nothing else.
func TestPresignPureURL(t *testing.T) {
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(newTestServer(t)),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	bfs := s3fs.New(testBucket,
		s3fs.WithClient(client),
		s3fs.WithPresignClient(s3.NewPresignClient(client)))

	req, err := bfs.PresignPut("pure.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(req.URL)
	if err != nil {
		t.Fatal(err)
	}
	if sh := u.Query().Get("X-Amz-SignedHeaders"); sh != "host" {
		t.Fatalf("PUT signed headers = %q, want host only", sh)
	}
	resp := doSigned(t, req.Method, req.URL, strings.NewReader("pure"))
	resp.Body.Close()
	if got := readFull(t, bfs, "pure.txt"); got != "pure" {
		t.Fatalf("content = %q, want %q", got, "pure")
	}

	greq, err := bfs.PresignGet("pure.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	gu, err := url.Parse(greq.URL)
	if err != nil {
		t.Fatal(err)
	}
	if sh := gu.Query().Get("X-Amz-SignedHeaders"); sh != "host" {
		t.Fatalf("GET signed headers = %q, want host only", sh)
	}
}

// TestPresignEndpoint signs URLs against a public endpoint distinct from
// the one the filesystem itself uses, as when the server reaches S3 over
// an internal address.
func TestPresignEndpoint(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket(testBucket); err != nil {
		t.Fatal(err)
	}
	internal := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(internal.Close)
	public := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(public.Close)

	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(internal.URL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	bfs := s3fs.New(testBucket,
		s3fs.WithClient(client),
		s3fs.WithPresignClient(s3.NewPresignClient(client, s3fs.WithPresignEndpoint(public.URL))))

	req, err := bfs.PresignPut("a.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(req.URL, public.URL+"/") {
		t.Fatalf("PUT URL %q does not target the presign endpoint %q", req.URL, public.URL)
	}
	resp := doSigned(t, req.Method, req.URL, strings.NewReader("via public"))
	resp.Body.Close()
	if got := readFull(t, bfs, "a.txt"); got != "via public" {
		t.Fatalf("content = %q, want %q", got, "via public")
	}

	greq, err := bfs.PresignGet("a.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(greq.URL, public.URL+"/") {
		t.Fatalf("GET URL %q does not target the presign endpoint %q", greq.URL, public.URL)
	}
	gresp := doSigned(t, greq.Method, greq.URL, nil)
	defer gresp.Body.Close()
	data, err := io.ReadAll(gresp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "via public" {
		t.Fatalf("body = %q", data)
	}
}

func TestPresignExpiry(t *testing.T) {
	bfs := newTestFS(t)
	writeFull(t, bfs, "a.txt", "content")

	req, err := bfs.PresignGet("a.txt", 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(req.URL, "X-Amz-Expires=7200") {
		t.Fatalf("URL %q does not carry the 2h expiry", req.URL)
	}
}
