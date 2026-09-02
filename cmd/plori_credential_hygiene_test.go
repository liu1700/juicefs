//go:build plori
// +build plori

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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/plori/creds"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/juicedata/juicefs/pkg/plori/mountspec"
)

// The audit's fixture credential. Every assertion below greps for these two
// strings, so they are shaped so nothing else in a JuiceFS binary could
// produce them by accident.
const (
	hygieneKeyID  = "PLORIHYGIENEKEYID001"
	hygieneSecret = "plori-hygiene-secret-Zk4-must-never-appear-anywhere"
)

// tinyS3 is an in-memory S3 that records how every request was signed and what
// every request carried. It is the wire half of the audit: the store sees the
// access key ID (SigV4 puts it in the Authorization header in cleartext, which
// is what an access key ID is for) and must never see the secret.
type tinyS3 struct {
	srv *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	traffic []string
	// accept, when non-empty, is the ONLY access key id this store honours.
	// Everything else answers 403 InvalidAccessKeyId, the way a real store
	// answers a key that has been regenerated out from under its holder. A fake
	// that accepts any signature cannot tell a rotation from a no-op, so the
	// negative tests set this and the hygiene test leaves it empty.
	accept string
}

func (s *tinyS3) onlyAccept(keyID string) {
	s.mu.Lock()
	s.accept = keyID
	s.mu.Unlock()
}

func (s *tinyS3) refuses(auth string) bool {
	s.mu.Lock()
	want := s.accept
	s.mu.Unlock()
	if want == "" {
		return false
	}
	return !strings.Contains(auth, "Credential="+want+"/")
}

func newTinyS3(t *testing.T) *tinyS3 {
	t.Helper()
	s := &tinyS3{objects: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		key := strings.TrimPrefix(r.URL.Path, "/")

		s.mu.Lock()
		s.traffic = append(s.traffic, r.Method+" "+r.URL.String())
		for name, values := range r.Header {
			for _, v := range values {
				s.traffic = append(s.traffic, name+": "+v)
			}
		}
		s.traffic = append(s.traffic, string(body))
		s.mu.Unlock()
		if s.refuses(r.Header.Get("Authorization")) {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InvalidAccessKeyId</Code><Message>The access key id you provided does not exist in our records.</Message></Error>`)
			return
		}
		s.mu.Lock()
		switch r.Method {
		case http.MethodPut:
			s.objects[key] = body
		case http.MethodDelete:
			delete(s.objects, key)
		}
		stored, ok := s.objects[key]
		s.mu.Unlock()

		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case http.MethodGet, http.MethodHead:
			if r.URL.Query().Has("list-type") {
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(stored)))
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write(stored)
			}
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *tinyS3) wire() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.traffic, "\n")
}

func (s *tinyS3) stored() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for k, v := range s.objects {
		b.WriteString(k)
		b.WriteString("=")
		b.Write(v)
		b.WriteString("\n")
	}
	return b.String()
}

func writeHygieneCredential(t *testing.T, path, keyID, secret string) {
	t.Helper()
	body := fmt.Sprintf(`{"access_key_id":%q,"secret_access_key":%q}`, keyID, secret)
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatalf("rename credential: %v", err)
	}
}

func hygieneSource(t *testing.T) *creds.Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "object-key.json")
	writeHygieneCredential(t, path, hygieneKeyID, hygieneSecret)
	src, err := creds.FromFile(path)
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	return src
}

// TestCredentialNeverReachesTheReplicatedMetadata formats a real volume
// against a real (tiny) S3 and then greps every artefact the format produced
// for the fixture key.
//
// This is the audit's central claim executed rather than argued. The Format
// document, the SQLite database it is stored in, and therefore every LTX frame
// Litestream makes out of that database are one surface: the credential fields
// on meta.Format are the only path a key could take into any of them, and the
// only thing that writes those fields in this binary is credentialPatch, which
// writes a constant.
func TestCredentialNeverReachesTheReplicatedMetadata(t *testing.T) {
	store := newTinyS3(t)
	source := hygieneSource(t)
	object.SetS3CredentialsProvider(source.Provider())
	t.Cleanup(func() { object.SetS3CredentialsProvider(nil) })

	dir := t.TempDir()
	paths := pmount.Paths{StateDir: dir, CacheDir: filepath.Join(dir, "cache"), MountPoint: filepath.Join(dir, "mnt")}
	fs := &ploriFS{
		paths:       paths,
		opts:        pmount.MountOptions{BufferSizeMB: 32},
		credentials: pmount.NewCredentialWatcher(source, func(string, ...any) {}),
	}
	spec := &mountspec.MountSpec{
		StorageVolumeID: "550e8400-e29b-41d4-a716-446655440000",
		FenceEpoch:      1,
		Format: mountspec.FormatSpec{
			Bucket:        store.srv.URL + "/plorifs",
			CapacityBytes: 1 << 30,
			Inodes:        100000,
			TrashDays:     1,
		},
	}
	if err := fs.Format(context.Background(), spec); err != nil {
		t.Fatalf("format: %v", err)
	}

	// 1. The Format document, as `juicefs dump` and `juicefs status` render it.
	m := meta.NewClient("sqlite3://"+paths.MetaPath(), nil)
	defer m.Shutdown() //nolint:errcheck
	format, err := m.Load(true)
	if err != nil {
		t.Fatalf("load format: %v", err)
	}
	if format.AccessKey != "" || format.SecretKey != "" || format.SessionToken != "" {
		t.Fatalf("the stored Format carries a credential: ak=%q sk=%q token=%q",
			format.AccessKey, format.SecretKey, format.SessionToken)
	}
	rendered, err := json.Marshal(format)
	if err != nil {
		t.Fatalf("marshal format: %v", err)
	}
	assertNoCredential(t, "Format JSON", string(rendered))

	// 2. The SQLite database, byte for byte. This is also the LTX surface:
	//    Litestream replicates these pages and nothing else.
	db, err := os.ReadFile(paths.MetaPath())
	if err != nil {
		t.Fatalf("read metadata database: %v", err)
	}
	assertNoCredential(t, "the SQLite metadata database", string(db))
	for _, sidecar := range []string{"-wal", "-shm"} {
		if extra, err := os.ReadFile(paths.MetaPath() + sidecar); err == nil {
			assertNoCredential(t, "the SQLite"+sidecar, string(extra))
		}
	}

	// 3. Everything that reached the object store, and everything it kept.
	if !strings.Contains(store.wire(), "Credential="+hygieneKeyID) {
		t.Fatalf("the store never saw a request signed with the fixture key, so this test proved nothing:\n%s", store.wire())
	}
	if strings.Contains(store.wire(), hygieneSecret) {
		t.Fatal("the secret key was transmitted to the object store")
	}
	assertNoCredential(t, "the objects the store kept", store.stored())
}

// TestTheInMemoryFormatCarriesAPlaceholderNotAKey pins the mechanism the test
// above depends on. If a future edit put the live key back into the in-memory
// Format, the scrub before Init would be the only thing standing between it
// and the replica — and NewReloadableStorage's reload log, which prints the
// access key it sees, would print a live one.
func TestTheInMemoryFormatCarriesAPlaceholderNotAKey(t *testing.T) {
	source := hygieneSource(t)
	fs := &ploriFS{credentials: pmount.NewCredentialWatcher(source, func(string, ...any) {})}
	var f meta.Format
	fs.credentialPatch()(&f)
	if f.AccessKey != credentialSentinel || f.SecretKey != credentialSentinel {
		t.Fatalf("the patch injected something other than the sentinel: ak=%q sk=%q", f.AccessKey, f.SecretKey)
	}
	if f.AccessKey == hygieneKeyID || f.SecretKey == hygieneSecret {
		t.Fatal("the patch injected the live credential")
	}
	if f.KeyEncrypted {
		t.Fatal("KeyEncrypted must be cleared or Format.Decrypt will try to AES-open the placeholder")
	}
	// The patch must be idempotent across reloads, or NewReloadableStorage
	// would rebuild the storage client on every heartbeat.
	before := [3]string{f.AccessKey, f.SecretKey, f.SessionToken}
	fs.credentialPatch()(&f)
	if after := [3]string{f.AccessKey, f.SecretKey, f.SessionToken}; after != before {
		t.Fatal("the patch is not idempotent, so a reload would look like a configuration change")
	}
}

// TestNoCommandLineFlagCanCarryACredential is the argv surface. The plugin
// builds the worker's command line, and a flag that took a key would put it in
// /proc/<pid>/cmdline for anything on the node that can read it.
func TestNoCommandLineFlagCanCarryACredential(t *testing.T) {
	var names []string
	var hasCredentialFile bool
	for _, f := range cmdPloriMount().Flags {
		for _, n := range f.Names() {
			names = append(names, n)
			lower := strings.ToLower(n)
			if lower == "credential-file" {
				hasCredentialFile = true
				continue
			}
			for _, banned := range []string{"access-key", "secret-key", "secret-access-key", "session-token", "password"} {
				if lower == banned {
					t.Errorf("plori-mount must not accept --%s: an argv is world-readable on the node", n)
				}
			}
		}
	}
	if !hasCredentialFile {
		t.Fatalf("plori-mount must accept --credential-file; flags are %v", names)
	}
}

// TestTheEnvironmentPathStillWorksAndSaysItCannotRotate keeps the pre-rotation
// delivery working while the CSI node plugin catches up (PLO-369), and makes
// the difference visible rather than silent.
func TestTheEnvironmentPathStillWorksAndSaysItCannotRotate(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", hygieneKeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", hygieneSecret)
	src, err := objectCredential("")
	if err != nil {
		t.Fatalf("objectCredential: %v", err)
	}
	if src.Rotates() {
		t.Fatal("the environment path cannot rotate and must not claim it can")
	}
	if got := src.Current(); got.AccessKeyID != hygieneKeyID || got.SecretAccessKey != hygieneSecret {
		t.Fatalf("the environment pair was not picked up: %+v", got)
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, err := objectCredential(""); err == nil {
		t.Fatal("a worker with no credential at all must refuse to start")
	} else if strings.Contains(err.Error(), hygieneSecret) {
		t.Fatalf("the refusal quotes the credential: %v", err)
	}
}

// TestACredentialFileErrorNeverQuotesTheFile closes the log surface at the one
// place a decoder would normally quote its input.
func TestACredentialFileErrorNeverQuotesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object-key.json")
	if err := os.WriteFile(path, []byte(hygieneSecret), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := objectCredential(path)
	if err == nil {
		t.Fatal("a credential file that is not a credential document must be refused")
	}
	assertNoCredential(t, "the refusal message", err.Error())
}

// TestAStolenOldPairIsRefusedAndTheLiveWorkerIsNot is the negative test the
// issue asks for, run against a store that actually CHECKS the key rather than
// a `file://` fake that accepts anything.
//
// It is one scenario with two halves, because only together do they mean
// anything: after the store moves to a new pair, a holder of the old pair —
// which is what a stolen credential is, once a rotation has happened — is
// refused, AND the live worker, which rotated, is not. A test that showed only
// the first would also pass if rotation were broken.
//
// The other negative case the issue lists, replay from another Pod, is not
// duplicated here: it is the writer lease and the conditional-PUT fence marker,
// and it is already proved by PLO-323's
// mount.TestASecondHolderAtTheSameEpochNeverMounts and
// mount.TestTheMarkerReclaimFailsClosedOnEveryUnprovenCase. Credentials have no
// part in it — on a store with one principal they cannot.
func TestAStolenOldPairIsRefusedAndTheLiveWorkerIsNot(t *testing.T) {
	store := newTinyS3(t)
	store.onlyAccept(hygieneKeyID)

	path := filepath.Join(t.TempDir(), "object-key.json")
	writeHygieneCredential(t, path, hygieneKeyID, hygieneSecret)
	source, err := creds.FromFile(path)
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	object.SetS3CredentialsProvider(source.Provider())
	t.Cleanup(func() { object.SetS3CredentialsProvider(nil) })

	// A client built the way the worker builds its own: through pkg/object, off
	// the shared provider.
	live, err := object.CreateStorage("s3", store.srv.URL+"/plorifs", credentialSentinel, credentialSentinel, "")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	ctx := context.Background()
	if err := live.Put(ctx, "before", strings.NewReader("x")); err != nil {
		t.Fatalf("the live worker cannot reach the store before the rotation: %v", err)
	}

	// The store's key is regenerated. On Vultr that is one call and the old
	// pair is dead from that instant — there is no overlap window (PLO-351).
	const stolenKeyID, stolenSecret = hygieneKeyID, hygieneSecret
	const newKeyID, newSecret = "PLORIHYGIENEKEYID002", "plori-hygiene-secret-rotated-Wr7"
	store.onlyAccept(newKeyID)

	// Half one: the old pair, held by anyone who captured it, is refused.
	stolenSource, err := creds.Static(stolenKeyID, stolenSecret)
	if err != nil {
		t.Fatalf("Static: %v", err)
	}
	object.SetS3CredentialsProvider(stolenSource.Provider())
	stolen, err := object.CreateStorage("s3", store.srv.URL+"/plorifs", credentialSentinel, credentialSentinel, "")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	err = stolen.Put(ctx, "stolen", strings.NewReader("x"))
	if err == nil {
		t.Fatal("the store accepted a pair it has regenerated away from")
	}
	if !pmount.IsCredentialRejected(err) {
		t.Fatalf("a regenerated-away pair must classify as a credential rejection, got %v", err)
	}

	// Half two: the worker that rotated keeps working, through the SAME client
	// object it was already using — no remount, no reconstruction.
	object.SetS3CredentialsProvider(source.Provider())
	writeHygieneCredential(t, path, newKeyID, newSecret)
	rotated, err := source.Reload()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !rotated {
		t.Fatal("the new pair was not picked up")
	}
	if err := live.Put(ctx, "after", strings.NewReader("x")); err != nil {
		t.Fatalf("the live worker did not follow the rotation: %v", err)
	}
}

func assertNoCredential(t *testing.T, surface, content string) {
	t.Helper()
	if strings.Contains(content, hygieneSecret) {
		t.Errorf("%s contains the secret key", surface)
	}
	if strings.Contains(content, hygieneKeyID) {
		t.Errorf("%s contains the access key id", surface)
	}
}
