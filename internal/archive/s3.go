package archive

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// S3 archives into a bucket of an S3-compatible store, by path-style
// requests (https://endpoint/bucket/key) signed with AWS Signature Version 4.
// Every PUT carries Content-MD5 and the payload's SHA-256, so the store
// refuses a body that arrives damaged.
type S3 struct {
	endpoint *url.URL
	bucket   string
	prefix   string // "" or ending in "/"
	region   string
	creds    Credentials
	http     *http.Client
	now      func() time.Time
}

func newS3(u *url.URL, creds Credentials) (*S3, error) {
	if u.Host == "" {
		return nil, errors.New("archive: s3:// needs a bucket, as s3://bucket/prefix")
	}
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return nil, errors.New("archive: an s3:// archive needs credentials (WORKFLOWD_ARCHIVE_ACCESS_KEY_ID and WORKFLOWD_ARCHIVE_SECRET_ACCESS_KEY, or --archive-credentials-file)")
	}
	q := u.Query()
	region := q.Get("region")
	endpoint := q.Get("endpoint")
	if endpoint == "" {
		if region == "" {
			region = "us-east-1"
		}
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	if region == "" {
		region = "us-east-1"
		if strings.Contains(endpoint, ".r2.cloudflarestorage.com") {
			region = "auto"
		}
	}
	ep, err := url.Parse(endpoint)
	if err != nil || ep.Host == "" || (ep.Scheme != "https" && ep.Scheme != "http") {
		return nil, fmt.Errorf("archive: endpoint %q: want https://host", endpoint)
	}
	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3{endpoint: ep, bucket: u.Host, prefix: prefix, region: region, creds: creds,
		http: &http.Client{Timeout: 2 * time.Minute}, now: time.Now}, nil
}

func (s *S3) String() string {
	return fmt.Sprintf("s3://%s/%s (%s, %s)", s.bucket, s.prefix, s.endpoint.Host, s.region)
}

// objectURL is the path-style URL of a key.
func (s *S3) objectURL(key string) *url.URL {
	u := *s.endpoint
	u.Path = "/" + s.bucket + "/" + s.prefix + key
	u.RawPath = "/" + s.bucket + "/" + uriEncode(s.prefix+key, false)
	return &u
}

func (s *S3) Put(ctx context.Context, key string, data []byte) error {
	sum := md5.Sum(data)
	headers := map[string]string{
		"content-md5":  base64.StdEncoding.EncodeToString(sum[:]),
		"content-type": "application/octet-stream",
	}
	res, err := s.do(ctx, http.MethodPut, s.objectURL(key), headers, data)
	if err != nil {
		return fmt.Errorf("archive: put %s: %w", key, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("archive: put %s: %w", key, s3Error(res))
	}
	// A single-part upload's ETag is the body's MD5 on S3 and R2: one more
	// check that what is stored is what was sent.
	if etag := strings.Trim(res.Header.Get("ETag"), `"`); len(etag) == 32 && etag != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("archive: put %s: the store answered ETag %s for a body whose MD5 is %x", key, etag, sum)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := s.do(ctx, http.MethodGet, s.objectURL(key), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("archive: get %s: %w", key, err)
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, fmt.Errorf("archive: get %s: %w", key, err)
		}
		return data, nil
	case http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	default:
		return nil, fmt.Errorf("archive: get %s: %w", key, s3Error(res))
	}
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	token := ""
	for {
		u := *s.endpoint
		u.Path = "/" + s.bucket
		q := url.Values{"list-type": {"2"}, "prefix": {s.prefix + prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		u.RawQuery = canonicalQuery(q)
		res, err := s.do(ctx, http.MethodGet, &u, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("archive: list: %w", err)
		}
		if res.StatusCode != http.StatusOK {
			err := s3Error(res)
			res.Body.Close()
			return nil, fmt.Errorf("archive: list: %w", err)
		}
		var page struct {
			Contents []struct {
				Key string `xml:"Key"`
			} `xml:"Contents"`
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		err = xml.NewDecoder(io.LimitReader(res.Body, 64<<20)).Decode(&page)
		res.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("archive: list: %w", err)
		}
		for _, c := range page.Contents {
			keys = append(keys, strings.TrimPrefix(c.Key, s.prefix))
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			break
		}
		token = page.NextContinuationToken
	}
	sort.Strings(keys)
	return keys, nil
}

// do sends one signed request.
func (s *S3) do(ctx context.Context, method string, u *url.URL, headers map[string]string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payloadHash := sha256Hex(body)
	h := map[string]string{"host": u.Host, "x-amz-content-sha256": payloadHash}
	for k, v := range headers {
		h[k] = v
	}
	auth := signV4(s.creds, s.region, "s3", s.now(), method, u.EscapedPath(), u.Query(), h, payloadHash)
	for k, v := range h {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", auth)
	req.ContentLength = int64(len(body))
	return s.http.Do(req)
}

// s3Error reads an S3 error reply (<Error><Code/><Message/></Error>).
func s3Error(res *http.Response) error {
	var e struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	data, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if xml.Unmarshal(data, &e) == nil && e.Code != "" {
		return fmt.Errorf("HTTP %d %s: %s", res.StatusCode, e.Code, e.Message)
	}
	return fmt.Errorf("HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(data)))
}

// signV4 returns the Authorization header for a request, by AWS Signature
// Version 4, and sets x-amz-date in headers (lower-case names, host
// included). path is the request's escaped path, exactly as sent.
func signV4(c Credentials, region, service string, now time.Time, method, path string, query url.Values,
	headers map[string]string, payloadHash string) string {
	amzDate := now.UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	headers["x-amz-date"] = amzDate

	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, strings.ToLower(k))
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k + ":" + strings.Join(strings.Fields(headers[k]), " ") + "\n")
	}
	signed := strings.Join(names, ";")
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{method, path, canonicalQuery(query), canonHeaders.String(), signed, payloadHash}, "\n")

	scope := date + "/" + region + "/" + service + "/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonical))
	key := hmacSHA256([]byte("AWS4"+c.SecretAccessKey), date)
	for _, part := range []string{region, service, "aws4_request"} {
		key = hmacSHA256(key, part)
	}
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	return "AWS4-HMAC-SHA256 Credential=" + c.AccessKeyID + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + sig
}

// canonicalQuery is SigV4's query string: names and values URI-encoded,
// sorted by name then value.
func canonicalQuery(q url.Values) string {
	var pairs []string
	for k, vs := range q {
		for _, v := range vs {
			pairs = append(pairs, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// uriEncode percent-encodes everything but RFC 3986's unreserved characters
// (and "/" in a path, when encodeSlash is false), as SigV4 requires.
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
