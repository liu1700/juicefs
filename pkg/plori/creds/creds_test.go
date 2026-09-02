/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package creds

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The fixture pair. Every hygiene assertion in this file greps for
// fixtureSecret, so it is deliberately a string nothing else could produce.
const (
	fixtureKeyID  = "PLORIFIXTUREKEYID001"
	fixtureSecret = "plori-fixture-secret-8Xq2-never-in-any-output"
	rotatedKeyID  = "PLORIFIXTUREKEYID002"
	rotatedSecret = "plori-rotated-secret-4Vz9-never-in-any-output"
)

func writeCredential(t *testing.T, path, keyID, secret string) {
	t.Helper()
	body := fmt.Sprintf(`{"access_key_id":%q,"secret_access_key":%q}`, keyID, secret)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	// Rename, so a reader never sees a half-written document. It is also what
	// the CSI node plugin must do, and writing the fixture any other way would
	// let a test pass that production could not.
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename credential: %v", err)
	}
}

func fileSource(t *testing.T) (*Source, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "object-key.json")
	writeCredential(t, path, fixtureKeyID, fixtureSecret)
	src, err := FromFile(path)
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	return src, path
}

// ---------------------------------------------------------------------------
// The rotation itself, end to end through a real signed request.
// ---------------------------------------------------------------------------

// signingRecorder is an S3 endpoint that remembers how each request was
// signed. The Authorization header names the access key id in cleartext (SigV4
// puts it in `Credential=<id>/<date>/...`) and proves the secret by a
// signature, never by sending it — so this is both the rotation oracle and the
// on-the-wire half of the hygiene audit.
type signingRecorder struct {
	srv *httptest.Server

	mu      sync.Mutex
	keyIDs  []string
	rawSeen []string
}

func newSigningRecorder(t *testing.T) *signingRecorder {
	t.Helper()
	r := &signingRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")
		r.mu.Lock()
		r.keyIDs = append(r.keyIDs, accessKeyIDFromAuth(auth))
		for name, values := range req.Header {
			for _, v := range values {
				r.rawSeen = append(r.rawSeen, name+": "+v)
			}
		}
		r.rawSeen = append(r.rawSeen, req.URL.String())
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func accessKeyIDFromAuth(auth string) string {
	const marker = "Credential="
	i := strings.Index(auth, marker)
	if i < 0 {
		return ""
	}
	rest := auth[i+len(marker):]
	if j := strings.Index(rest, "/"); j >= 0 {
		return rest[:j]
	}
	return rest
}

func (r *signingRecorder) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keyIDs...)
}

func (r *signingRecorder) everything() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.rawSeen, "\n")
}

func (r *signingRecorder) client(t *testing.T, p aws.CredentialsProvider) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(p),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(r.srv.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
}

// TestARotationReachesTheNextSignedRequest is the whole feature in one test:
// the key changes on disk, nothing is remounted, and the very next S3 request
// is signed with the new one.
//
// It goes through config.LoadDefaultConfig rather than calling Retrieve
// directly on purpose. The SDK wraps a credential provider in its own cache,
// and a cache is exactly what would make a rotation eventual instead of exact;
// this asserts that the provider this package hands out is already that cache,
// so LoadDefaultConfig leaves it alone and invalidating it is enough.
func TestARotationReachesTheNextSignedRequest(t *testing.T) {
	src, path := fileSource(t)
	rec := newSigningRecorder(t)
	client := rec.client(t, src.Provider())

	put := func() {
		t.Helper()
		_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String("plorifs"),
			Key:    aws.String("agents/v1/chunks/0/0/1"),
			Body:   strings.NewReader("block"),
		})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	put()
	put() // a second request under the same key: the cache must not re-retrieve

	writeCredential(t, path, rotatedKeyID, rotatedSecret)
	rotated, err := src.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !rotated {
		t.Fatal("a changed credential file must report a rotation")
	}
	put()

	got := rec.keys()
	want := []string{fixtureKeyID, fixtureKeyID, rotatedKeyID}
	if len(got) != len(want) {
		t.Fatalf("expected %d signed requests, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request %d signed with %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if src.Generation() != 2 {
		t.Fatalf("generation = %d, want 2", src.Generation())
	}
}

// TestNoSecretIsEverSentToTheStore is the wire surface of the hygiene audit.
// SigV4 proves possession of the secret with an HMAC; a request that carried
// the secret itself would be a leak to every hop in between.
func TestNoSecretIsEverSentToTheStore(t *testing.T) {
	src, _ := fileSource(t)
	rec := newSigningRecorder(t)
	client := rec.client(t, src.Provider())
	if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("plorifs"),
		Key:    aws.String("k"),
		Body:   strings.NewReader("v"),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if seen := rec.everything(); strings.Contains(seen, fixtureSecret) {
		t.Fatalf("the secret key reached the wire:\n%s", seen)
	}
}

// TestTheProviderIsTheSDKsCacheSoNothingWrapsItTwice pins the fact the design
// rests on. If a future SDK stopped recognising an existing cache and wrapped
// this one in a second one, Invalidate would clear the inner cache while the
// outer kept handing out the old pair, and rotation would silently stop
// working with every test above still green.
func TestTheProviderIsTheSDKsCacheSoNothingWrapsItTwice(t *testing.T) {
	src, _ := fileSource(t)
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(src.Provider()),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Credentials != src.Provider() {
		t.Fatalf("LoadDefaultConfig replaced the provider: %T", cfg.Credentials)
	}
}

// TestRetrieveNeverSeesAHalfRotatedPair hammers Retrieve while the file
// rotates underneath it. Every pair that comes out must be internally
// consistent: the id and the secret of one generation, never a mixture.
func TestRetrieveNeverSeesAHalfRotatedPair(t *testing.T) {
	src, path := fileSource(t)
	pairs := map[string]string{fixtureKeyID: fixtureSecret, rotatedKeyID: rotatedSecret}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, err := src.Provider().Retrieve(context.Background())
				if err != nil {
					t.Errorf("retrieve: %v", err)
					return
				}
				if want, ok := pairs[c.AccessKeyID]; !ok || want != c.SecretAccessKey {
					t.Errorf("half-rotated pair: id %q with secret %q", c.AccessKeyID, c.SecretAccessKey)
					return
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			writeCredential(t, path, rotatedKeyID, rotatedSecret)
		} else {
			writeCredential(t, path, fixtureKeyID, fixtureSecret)
		}
		if _, err := src.Reload(); err != nil {
			t.Fatalf("reload: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Fail-closed and fail-soft
// ---------------------------------------------------------------------------

func TestAStartupWithoutAUsableCredentialIsFatal(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		content string
		write   bool
	}{
		{name: "absent", write: false},
		{name: "not json", content: "AKIA...\n", write: true},
		{name: "empty id", content: `{"access_key_id":"","secret_access_key":"s"}`, write: true},
		{name: "empty secret", content: `{"access_key_id":"a","secret_access_key":""}`, write: true},
		{name: "empty document", content: `{}`, write: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.name+".json")
			if tc.write {
				if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			_, err := FromFile(path)
			if !errors.Is(err, ErrNoCredential) {
				t.Fatalf("want ErrNoCredential, got %v", err)
			}
		})
	}
}

// TestABadRefreshKeepsTheLastGoodPair is the fail-SOFT half. A projected
// volume is momentarily absent while the kubelet swaps it and a file being
// written is momentarily incomplete; dropping the working credential on either
// would turn a routine event into an outage. The store, not the file, is what
// gets to say a credential is dead — and that decision lives in the mount
// package's watcher, not here.
func TestABadRefreshKeepsTheLastGoodPair(t *testing.T) {
	src, path := fileSource(t)
	for _, tc := range []struct {
		name string
		mut  func()
	}{
		{"truncated json", func() { mustWrite(t, path, `{"access_key_id":"AK`) }},
		{"empty file", func() { mustWrite(t, path, "") }},
		{"removed", func() { os.Remove(path) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writeCredential(t, path, fixtureKeyID, fixtureSecret)
			if _, err := src.Reload(); err != nil {
				t.Fatalf("baseline reload: %v", err)
			}
			tc.mut()
			rotated, err := src.Reload()
			if err == nil {
				t.Fatal("a bad refresh must report an error")
			}
			if rotated {
				t.Fatal("a bad refresh is not a rotation")
			}
			if got := src.Current(); got.AccessKeyID != fixtureKeyID || got.SecretAccessKey != fixtureSecret {
				t.Fatalf("the last good pair was dropped: %+v", got)
			}
			at, why := src.StaleSince()
			if at.IsZero() || why == nil {
				t.Fatal("a bad refresh must be reported as stale")
			}
			if strings.Contains(why.Error(), fixtureSecret) || strings.Contains(why.Error(), "AK") {
				t.Fatalf("the refresh error quotes the file's content: %v", why)
			}
		})
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestAGoodRefreshClearsTheStaleMark(t *testing.T) {
	src, path := fileSource(t)
	mustWrite(t, path, "{")
	if _, err := src.Reload(); err == nil {
		t.Fatal("expected a refresh error")
	}
	writeCredential(t, path, rotatedKeyID, rotatedSecret)
	if _, err := src.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if at, _ := src.StaleSince(); !at.IsZero() {
		t.Fatalf("the stale mark survived a good refresh: %v", at)
	}
}

// TestAnUnchangedFileIsNotARotation matters because a rotation restarts the
// Litestream child. Re-reading the same bytes every ten seconds must not.
func TestAnUnchangedFileIsNotARotation(t *testing.T) {
	src, path := fileSource(t)
	for i := 0; i < 5; i++ {
		writeCredential(t, path, fixtureKeyID, fixtureSecret) // same content, new inode
		rotated, err := src.Reload()
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if rotated {
			t.Fatal("identical content is not a rotation")
		}
	}
	if src.Generation() != 1 {
		t.Fatalf("generation = %d, want 1", src.Generation())
	}
}

// ---------------------------------------------------------------------------
// The static path, and the environment surface
// ---------------------------------------------------------------------------

func TestAStaticSourceCannotRotate(t *testing.T) {
	src, err := Static(fixtureKeyID, fixtureSecret)
	if err != nil {
		t.Fatalf("Static: %v", err)
	}
	if src.Rotates() {
		t.Fatal("the environment path must report that it cannot rotate")
	}
	rotated, err := src.Reload()
	if err != nil || rotated {
		t.Fatalf("Reload on a static source: rotated=%v err=%v", rotated, err)
	}
	if _, err := Static("", fixtureSecret); !errors.Is(err, ErrNoCredential) {
		t.Fatalf("half a pair must be no credential, got %v", err)
	}
}

// TestTheProcessEnvironmentHoldsNoCredentialOnTheFilePath is one surface of
// the hygiene audit: with a credential file, the worker's own
// /proc/<pid>/environ has nothing in it, and only the child Litestream is
// started with the pair.
func TestTheProcessEnvironmentHoldsNoCredentialOnTheFilePath(t *testing.T) {
	src, _ := fileSource(t)

	for _, kv := range os.Environ() {
		if strings.Contains(kv, fixtureSecret) || strings.HasPrefix(kv, "AWS_SECRET_ACCESS_KEY=") {
			t.Fatalf("reading a credential file must not export it: %q", kv)
		}
	}

	child := src.Env([]string{"PATH=/usr/bin"})
	var sawID, sawSecret bool
	for _, kv := range child {
		switch kv {
		case "AWS_ACCESS_KEY_ID=" + fixtureKeyID:
			sawID = true
		case "AWS_SECRET_ACCESS_KEY=" + fixtureSecret:
			sawSecret = true
		}
	}
	if !sawID || !sawSecret {
		t.Fatalf("the child environment must carry the current pair: %v", child)
	}
}

// TestTheChildEnvironmentFollowsARotation proves the Litestream restart is
// worth doing: Env is read at start, so a restart after a rotation is what
// hands the child the new key.
func TestTheChildEnvironmentFollowsARotation(t *testing.T) {
	src, path := fileSource(t)
	writeCredential(t, path, rotatedKeyID, rotatedSecret)
	if _, err := src.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	child := strings.Join(src.Env(nil), "\n")
	if !strings.Contains(child, "AWS_ACCESS_KEY_ID="+rotatedKeyID) {
		t.Fatalf("the child environment kept the old key: %s", child)
	}
	if strings.Contains(child, fixtureSecret) {
		t.Fatalf("the child environment kept the old secret: %s", child)
	}
}

// TestRetrieveIsCheapEnoughToBeOnTheRequestPath guards the one performance
// property the design assumes: the SDK asks the provider once per request when
// the cache is cold, and Retrieve is a pointer load, not a file read.
func TestRetrieveIsCheapEnoughToBeOnTheRequestPath(t *testing.T) {
	src, path := fileSource(t)
	reads := 0
	src.readFile = func(p string) ([]byte, error) {
		reads++
		return os.ReadFile(p)
	}
	start := time.Now()
	for i := 0; i < 100000; i++ {
		if _, err := src.retrieve(context.Background()); err != nil {
			t.Fatalf("retrieve: %v", err)
		}
	}
	if reads != 0 {
		t.Fatalf("Retrieve read the file %d times; it must never touch the disk", reads)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("100k retrieves took %s", elapsed)
	}
	_ = path
}
