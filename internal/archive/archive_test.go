package archive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// AWS's own SigV4 test suite (as botocore ships it: aws4_testsuite), which
// fixes the algorithm independently of this implementation.
func TestSignV4AgainstTheAWSTestSuite(t *testing.T) {
	creds := Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	at := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	empty := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	for _, c := range []struct {
		name, method, query, payload string
		headers                      map[string]string
		want                         string
	}{
		{"get-vanilla", "GET", "", "", map[string]string{"host": "example.amazonaws.com"},
			"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"},
		{"get-vanilla-query-order-key-case", "GET", "Param2=value2&Param1=value1", "", map[string]string{"host": "example.amazonaws.com"},
			"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"},
		{"post-x-www-form-urlencoded", "POST", "", "Param1=value1",
			map[string]string{"host": "example.amazonaws.com", "content-type": "application/x-www-form-urlencoded"},
			"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a"},
	} {
		q, _ := url.ParseQuery(c.query)
		hash := empty
		if c.payload != "" {
			hash = sha256Hex([]byte(c.payload))
		}
		if got := signV4(creds, "us-east-1", "service", at, c.method, "/", q, c.headers, hash); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}

func TestURIEncode(t *testing.T) {
	for in, want := range map[string]string{
		"acme/order 1/0001~r.history": "acme/order%201/0001~r.history",
		"a+b=c&d$e":                   "a%2Bb%3Dc%26d%24e",
		"üñï":                         "%C3%BC%C3%B1%C3%AF",
	} {
		if got := uriEncode(in, false); got != want {
			t.Errorf("uriEncode(%q) = %q, want %q", in, got, want)
		}
	}
	if got := uriEncode("a/b", true); got != "a%2Fb" {
		t.Errorf("query encoding kept a slash: %q", got)
	}
}

func TestKeys(t *testing.T) {
	r := Run{Namespace: "acme", WorkflowID: "order 1~x", RunID: "9f3c", ClosedAt: 0x1234}
	if got := r.Key(); got != "acme/order 1~x/0000000000001234~9f3c.history" {
		t.Fatalf("key %q", got)
	}
	if back, ok := ParseKey(r.Key()); !ok || back != r {
		t.Fatalf("ParseKey = %+v, %v", back, ok)
	}
	for _, bad := range []string{"acme/x.history", "acme/x/12~r.history", "acme/x/zzzzzzzzzzzzzzzz~r.history", "x/y/0000000000001234~r.tmp"} {
		if _, ok := ParseKey(bad); ok {
			t.Errorf("ParseKey(%q) accepted", bad)
		}
	}
}

func TestDirStore(t *testing.T) {
	root := t.TempDir()
	d, err := Open("file://"+root, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Put(ctx, "acme/w 1/0001~r.history", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := d.Put(ctx, "default/w2/0002~s.history", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get(ctx, "acme/w 1/0001~r.history"); err != nil || string(got) != "one" {
		t.Fatalf("get = %q, %v", got, err)
	}
	if _, err := d.Get(ctx, "acme/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v", err)
	}
	keys, err := d.List(ctx, "acme/")
	if err != nil || !slices.Equal(keys, []string{"acme/w 1/0001~r.history"}) {
		t.Fatalf("list = %v, %v", keys, err)
	}
	for _, bad := range []string{"../escape", "a/../../b", "a//b", `a\b`} {
		if err := d.Put(ctx, bad, nil); err == nil {
			t.Errorf("put %q was accepted", bad)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape")); !os.IsNotExist(err) {
		t.Fatal("a key escaped the archive directory")
	}
}

// fakeS3 is an S3 that checks every request the way S3 does: the signature
// over the request as it arrived, the payload hash, and Content-MD5.
type fakeS3 struct {
	t       *testing.T
	creds   Credentials
	region  string
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	fail    bool // answer every PUT 500
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if r.Header.Get("x-amz-content-sha256") != sha256Hex(body) {
		http.Error(w, "<Error><Code>XAmzContentSHA256Mismatch</Code></Error>", http.StatusBadRequest)
		return
	}
	auth := r.Header.Get("Authorization")
	signedNames := auth[strings.Index(auth, "SignedHeaders=")+len("SignedHeaders=") : strings.Index(auth, ", Signature=")]
	headers := map[string]string{}
	for _, n := range strings.Split(signedNames, ";") {
		if n == "host" {
			headers[n] = r.Host
		} else if n != "x-amz-date" {
			headers[n] = r.Header.Get(n)
		}
	}
	at, _ := time.Parse("20060102T150405Z", r.Header.Get("x-amz-date"))
	// Like S3, canonicalize the path from its decoded form, not as it was sent.
	path := "/" + uriEncode(strings.TrimPrefix(r.URL.Path, "/"), false)
	want := signV4(f.creds, f.region, "s3", at, r.Method, path, r.URL.Query(), headers, sha256Hex(body))
	if auth != want {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<Error><Code>SignatureDoesNotMatch</Code><Message>signature</Message></Error>")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPut:
		sum := md5.Sum(body)
		if r.Header.Get("Content-MD5") != base64.StdEncoding.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "<Error><Code>BadDigest</Code></Error>")
			return
		}
		if f.fail {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "<Error><Code>InternalError</Code><Message>try again</Message></Error>")
			return
		}
		f.objects[r.URL.Path] = body
		f.puts++
		w.Header().Set("ETag", `"`+strings.ToLower(hexOf(sum[:]))+`"`)
	case r.URL.Query().Get("list-type") == "2":
		prefix := "/" + strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0] + "/" + r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, strings.TrimPrefix(k, "/"+strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0]+"/"))
			}
		}
		sort.Strings(keys)
		// Two keys a page, to exercise continuation.
		start := 0
		if tok := r.URL.Query().Get("continuation-token"); tok != "" {
			start = slices.Index(keys, tok)
		}
		type content struct {
			Key string `xml:"Key"`
		}
		var page struct {
			XMLName               xml.Name  `xml:"ListBucketResult"`
			Contents              []content `xml:"Contents"`
			IsTruncated           bool      `xml:"IsTruncated"`
			NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
		}
		for i := start; i < len(keys) && i < start+2; i++ {
			page.Contents = append(page.Contents, content{keys[i]})
		}
		if start+2 < len(keys) {
			page.IsTruncated, page.NextContinuationToken = true, keys[start+2]
		}
		xml.NewEncoder(w).Encode(page)
	case r.Method == http.MethodGet:
		data, ok := f.objects[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		w.Write(data)
	}
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return string(out)
}

func TestS3Store(t *testing.T) {
	creds := Credentials{AccessKeyID: "AK", SecretAccessKey: "secret"}
	fake := &fakeS3{t: t, creds: creds, region: "auto", objects: map[string][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	st, err := Open("s3://bucket/workflowd/prod?region=auto&endpoint="+url.QueryEscape(srv.URL), creds)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	keys := []string{"acme/order 1/0001~a.history", "acme/order+2/0002~b.history", "acme/ü/0003~c.history", "beta/x/0004~d.history"}
	for _, k := range keys {
		if err := st.Put(ctx, k, []byte("data:"+k)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	if _, ok := fake.objects["/bucket/workflowd/prod/acme/order 1/0001~a.history"]; !ok {
		t.Fatalf("stored under %v", keys)
	}
	for _, k := range keys {
		if got, err := st.Get(ctx, k); err != nil || !bytes.Equal(got, []byte("data:"+k)) {
			t.Fatalf("get %s = %q, %v", k, got, err)
		}
	}
	if _, err := st.Get(ctx, "acme/none"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	got, err := st.List(ctx, "acme/")
	if err != nil || !slices.Equal(got, keys[:3]) {
		t.Fatalf("list across pages = %v, %v", got, err)
	}
	// A wrong secret is refused by the store, and says so.
	bad, _ := Open("s3://bucket/workflowd/prod?endpoint="+url.QueryEscape(srv.URL)+"&region=auto", Credentials{"AK", "wrong"})
	if err := bad.Put(ctx, "acme/x/0001~z.history", []byte("x")); err == nil || !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
		t.Fatalf("wrong secret: %v", err)
	}
	fake.fail = true
	if err := st.Put(ctx, "acme/y/0001~z.history", []byte("x")); err == nil || !strings.Contains(err.Error(), "InternalError") {
		t.Fatalf("a failing store: %v", err)
	}
}

func TestOpenSpecs(t *testing.T) {
	creds := Credentials{"a", "b"}
	for _, bad := range []string{"s3://", "ftp://x/y", "s3://b/p?endpoint=notaurl"} {
		if _, err := Open(bad, creds); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
	if _, err := Open("s3://b/p", Credentials{}); err == nil {
		t.Error("an s3 archive without credentials was accepted")
	}
	s, err := Open("s3://b/p?endpoint=https://acct.r2.cloudflarestorage.com", creds)
	if err != nil || s.(*S3).region != "auto" {
		t.Fatalf("R2 endpoint: %v %v", s, err)
	}
	if strings.Contains(s.String(), "b ") || strings.Contains(s.String(), creds.SecretAccessKey+"@") {
		t.Fatalf("String leaks: %s", s)
	}
}
