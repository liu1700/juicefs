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

package mount

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/juicedata/juicefs/pkg/plori/creds"
)

const (
	testKeyID     = "PLORIMOUNTKEYID0001"
	testSecret    = "plori-mount-fixture-secret-Qb7-never-in-any-output"
	testRotatedID = "PLORIMOUNTKEYID0002"
)

func credentialFile(t *testing.T, keyID, secret string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "object-key.json")
	writeCredentialFile(t, path, keyID, secret)
	return path
}

func writeCredentialFile(t *testing.T, path, keyID, secret string) {
	t.Helper()
	body := fmt.Sprintf(`{"access_key_id":%q,"secret_access_key":%q}`, keyID, secret)
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatalf("rename credential: %v", err)
	}
}

// capturedLog collects every line the worker would write to stderr, so a test
// can assert what is NOT in them.
type capturedLog struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturedLog) fn(event string, kv ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := []string{event}
	for _, v := range kv {
		parts = append(parts, fmt.Sprint(v))
	}
	c.lines = append(c.lines, strings.Join(parts, " "))
}

func (c *capturedLog) all() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

func testWatcher(t *testing.T, path string, log func(string, ...any)) *CredentialWatcher {
	t.Helper()
	src, err := creds.FromFile(path)
	if err != nil {
		t.Fatalf("FromFile: %v", err)
	}
	w := NewCredentialWatcher(src, log)
	w.grace = 30 * time.Millisecond
	w.poll = 5 * time.Millisecond
	return w
}

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

// TestTheCredentialVerdictTransitions is the whole fail-closed contract in one
// table. Two inputs — what the file says and what the store says — and the one
// output the supervisor acts on.
func TestTheCredentialVerdictTransitions(t *testing.T) {
	// A fixed clock: every row that depends on the grace says so by moving it.
	base := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

	rejected := func(err bool) error {
		if !err {
			return nil
		}
		return &smithy.GenericAPIError{Code: "InvalidAccessKeyId", Message: "the key is not valid"}
	}

	for _, tc := range []struct {
		name string
		// steps run in order against a watcher whose file starts valid.
		steps func(t *testing.T, w *CredentialWatcher, path string, clock *time.Time)
		want  CredentialVerdict
	}{
		{
			name:  "a readable file the store accepts is healthy",
			steps: func(*testing.T, *CredentialWatcher, string, *time.Time) {},
			want:  CredentialOK,
		},
		{
			name: "a refresh that cannot read the file keeps serving and says so",
			steps: func(t *testing.T, w *CredentialWatcher, path string, _ *time.Time) {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				w.Poll()
			},
			want: CredentialStale,
		},
		{
			name: "a malformed file keeps serving and says so",
			steps: func(t *testing.T, w *CredentialWatcher, path string, _ *time.Time) {
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
				w.Poll()
			},
			want: CredentialStale,
		},
		{
			name: "a readable file after a bad one is healthy again",
			steps: func(t *testing.T, w *CredentialWatcher, path string, _ *time.Time) {
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
				w.Poll()
				writeCredentialFile(t, path, testRotatedID, testSecret)
				w.Poll()
			},
			want: CredentialOK,
		},
		{
			name: "a rejection inside the grace is not yet a reason to stop",
			steps: func(_ *testing.T, w *CredentialWatcher, _ string, clock *time.Time) {
				w.Observe(rejected(true))
				*clock = clock.Add(w.grace - time.Millisecond)
			},
			want: CredentialStale,
		},
		{
			name: "a rejection past the grace stops the worker",
			steps: func(_ *testing.T, w *CredentialWatcher, _ string, clock *time.Time) {
				w.Observe(rejected(true))
				*clock = clock.Add(w.grace)
			},
			want: CredentialRejected,
		},
		{
			name: "a successful operation clears the rejection",
			steps: func(_ *testing.T, w *CredentialWatcher, _ string, clock *time.Time) {
				w.Observe(rejected(true))
				*clock = clock.Add(w.grace / 2)
				w.Observe(nil)
				*clock = clock.Add(w.grace)
			},
			want: CredentialOK,
		},
		{
			name: "a rotation gives the new key its own grace",
			steps: func(t *testing.T, w *CredentialWatcher, path string, clock *time.Time) {
				w.Observe(rejected(true))
				*clock = clock.Add(w.grace - time.Millisecond)
				writeCredentialFile(t, path, testRotatedID, testSecret)
				if !w.Poll() {
					t.Fatal("a changed file must be a rotation")
				}
				*clock = clock.Add(w.grace - time.Millisecond)
			},
			want: CredentialOK,
		},
		{
			name: "an error that is not about the credential is ignored",
			steps: func(_ *testing.T, w *CredentialWatcher, _ string, clock *time.Time) {
				w.Observe(errors.New("connection reset by peer"))
				w.Observe(&smithy.GenericAPIError{Code: "SlowDown"})
				*clock = clock.Add(10 * w.grace)
			},
			want: CredentialOK,
		},
		{
			name: "a rejection that outlives the grace with the file still readable still stops",
			steps: func(t *testing.T, w *CredentialWatcher, path string, clock *time.Time) {
				w.Observe(rejected(true))
				w.Poll() // the file is fine; the STORE is what refuses
				*clock = clock.Add(w.grace)
			},
			want: CredentialRejected,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := credentialFile(t, testKeyID, testSecret)
			log := &capturedLog{}
			w := testWatcher(t, path, log.fn)
			clock := base
			w.now = func() time.Time { return clock }

			tc.steps(t, w, path, &clock)

			if got := w.Verdict(); got != tc.want {
				t.Fatalf("verdict = %q, want %q", got, tc.want)
			}
			if strings.Contains(log.all(), testSecret) {
				t.Fatalf("the secret reached a log line:\n%s", log.all())
			}
		})
	}
}

func TestIsCredentialRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"invalid access key", &smithy.GenericAPIError{Code: "InvalidAccessKeyId"}, true},
		{"bad signature", &smithy.GenericAPIError{Code: "SignatureDoesNotMatch"}, true},
		{"access denied", &smithy.GenericAPIError{Code: "AccessDenied"}, true},
		{"expired token", &smithy.GenericAPIError{Code: "ExpiredToken"}, true},
		{"no credential at all", creds.ErrNoCredential, true},
		{"wrapped no credential", fmt.Errorf("put: %w", creds.ErrNoCredential), true},
		{"a bare 403", statusErr(http.StatusForbidden), true},
		{"a 404 is not a credential problem", statusErr(http.StatusNotFound), false},
		{"a 412 is the fence, not the credential", statusErr(http.StatusPreconditionFailed), false},
		{"throttling", &smithy.GenericAPIError{Code: "SlowDown"}, false},
		{"a plain network error", errors.New("dial tcp: i/o timeout"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCredentialRejected(tc.err); got != tc.want {
				t.Fatalf("IsCredentialRejected(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type httpStatusErr int

func (e httpStatusErr) Error() string       { return fmt.Sprintf("http %d", int(e)) }
func (e httpStatusErr) HTTPStatusCode() int { return int(e) }

func statusErr(code int) error { return httpStatusErr(code) }

// ---------------------------------------------------------------------------
// What the supervisor does about it
// ---------------------------------------------------------------------------

// reloadingReplicator is a fakeReplicator that also implements
// ReplicatorReloader, which is how the Litestream child is restarted onto a
// new key.
type reloadingReplicator struct {
	fakeReplicator
	reloads chan struct{}
	err     error
}

func (r *reloadingReplicator) ReloadCredentials(context.Context) error {
	r.record("reload_credentials")
	select {
	case r.reloads <- struct{}{}:
	default:
	}
	return r.err
}

// TestARotationRestartsTheReplicator: the data path picks the new key up by
// itself (the provider is shared), but Litestream read its key from an
// environment it can no longer be handed, so the only mechanism is a restart.
func TestARotationRestartsTheReplicator(t *testing.T) {
	path := credentialFile(t, testKeyID, testSecret)
	log := &capturedLog{}
	w := testWatcher(t, path, log.fn)

	rep := &reloadingReplicator{reloads: make(chan struct{}, 4)}
	cp := &fakeCP{}
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.Deps.Credentials = w
	sup.Deps.Log = log.fn

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	// Let the mount come up, then roll the key.
	waitFor(t, 10*time.Second, func() bool { return exists(t, sup.Paths.ReadyPath()) }, "timed out waiting for the mount to become ready")
	writeCredentialFile(t, path, testRotatedID, testSecret)

	select {
	case <-rep.reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("a rotation did not restart the replicator")
	}
	if got := w.Generation(); got != 2 {
		t.Fatalf("generation = %d, want 2", got)
	}

	stop <- os.Interrupt
	if f := <-done; f.Exit != CodeOK {
		t.Fatalf("exit = %d, want a clean stop (%v)", f.Exit, f.Err)
	}
	if strings.Contains(log.all(), testSecret) {
		t.Fatalf("the secret reached a log line:\n%s", log.all())
	}
}

// TestARejectedCredentialStopsWithoutTouchingTheStore is the fail-closed half.
//
// The shape is the out-of-band fence's — detach, no barrier, no final sync,
// abort the replicator — and the reason it must be is not authority but
// reachability: a barrier and a final sync are both writes to the store that
// is refusing this process, so running them would burn the whole remaining
// lease failing and report data loss (exit 69) for a condition the plugin can
// simply retry.
func TestARejectedCredentialStopsWithoutTouchingTheStore(t *testing.T) {
	path := credentialFile(t, testKeyID, testSecret)
	log := &capturedLog{}
	w := testWatcher(t, path, log.fn)

	rep := &fakeReplicator{}
	cp := &fakeCP{}
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, rep, &fakeFencer{})
	sup.Deps.Credentials = w
	sup.Deps.Log = log.fn

	// The store has been refusing this key for longer than the grace.
	w.Observe(&smithy.GenericAPIError{Code: "InvalidAccessKeyId"})
	w.now = func() time.Time { return time.Now().Add(2 * w.grace) }

	got := sup.Run(context.Background(), make(chan os.Signal))

	if got.Exit != CodeObjectStore {
		t.Fatalf("exit = %d, want %d (%v)", got.Exit, CodeObjectStore, got.Err)
	}
	if got.ErrCode != ErrCodeObjectStoreUnreachable {
		t.Errorf("error code = %s, want %s", got.ErrCode, ErrCodeObjectStoreUnreachable)
	}
	if !got.Retryable {
		t.Error("a credential the node may already have replaced is retryable")
	}
	for _, forbidden := range []string{"barrier", "unmount"} {
		if contains(vol.order(), forbidden) {
			t.Errorf("the stop called %s, which needs the store that is refusing us: %v", forbidden, vol.order())
		}
	}
	if !contains(vol.order(), "detach") {
		t.Errorf("the mount must be detached: %v", vol.order())
	}
	if contains(rep.order(), "sync") {
		t.Errorf("the final sync must be skipped: %v", rep.order())
	}
	if !contains(rep.order(), "abort") {
		t.Errorf("the replicator must be aborted: %v", rep.order())
	}
	if cp.released != ReasonCredentialRejected {
		t.Errorf("release reason = %q, want %q", cp.released, ReasonCredentialRejected)
	}
	if contains(cp.order(), "durable_point") {
		t.Errorf("no barrier ran, so there is no durable point to report: %v", cp.order())
	}
	if exists(t, sup.Paths.CleanStopPath()) {
		t.Error("a credential stop is not a clean stop; the next generation must repair")
	}
}

// TestTheExistingStopShapesAreUnchanged guards the one-line edit that taught
// shutdown about a third reason. The clean stop and the out-of-band fence are
// PLO-323/326's contract and must be byte-for-byte what they were.
func TestTheExistingStopShapesAreUnchanged(t *testing.T) {
	t.Run("clean stop still runs the barrier and the final sync", func(t *testing.T) {
		rep := &fakeReplicator{}
		cp := &fakeCP{}
		vol := healthyVolume()
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, rep, &fakeFencer{})
		stop := make(chan os.Signal, 1)
		done := make(chan *Fatal, 1)
		go func() { done <- sup.Run(context.Background(), stop) }()
		waitFor(t, 10*time.Second, func() bool { return exists(t, sup.Paths.ReadyPath()) }, "timed out waiting for the mount to become ready")
		stop <- os.Interrupt
		if f := <-done; f.Exit != CodeOK {
			t.Fatalf("exit = %d, want 0 (%v)", f.Exit, f.Err)
		}
		if !contains(vol.order(), "barrier") || !contains(vol.order(), "unmount") {
			t.Fatalf("clean stop lost a step: %v", vol.order())
		}
		if !contains(rep.order(), "sync") || !contains(rep.order(), "stop") {
			t.Fatalf("clean stop lost a replication step: %v", rep.order())
		}
		if cp.released != ReasonShutdown {
			t.Fatalf("release reason = %q, want %q", cp.released, ReasonShutdown)
		}
	})

	t.Run("an out-of-band fence still exits 66", func(t *testing.T) {
		rep := &fakeReplicator{}
		cp := &fakeCP{renewErr: &CPError{Status: http.StatusConflict, Code: "stale_epoch"}}
		vol := healthyVolume()
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, rep, &fakeFencer{})
		got := sup.Run(context.Background(), make(chan os.Signal))
		if got.Exit != CodeFenced || got.ErrCode != ErrCodeFencedOutOfBand {
			t.Fatalf("exit = %d/%s, want %d/%s (%v)", got.Exit, got.ErrCode, CodeFenced, ErrCodeFencedOutOfBand, got.Err)
		}
		if cp.released != ReasonFencedOutOfBand {
			t.Fatalf("release reason = %q, want %q", cp.released, ReasonFencedOutOfBand)
		}
	})
}

// ---------------------------------------------------------------------------
// The reported surfaces
// ---------------------------------------------------------------------------

// TestHealthReportsTheCredentialWithoutNamingIt covers two of the audit's
// surfaces at once: health.json says enough for a rotation drill to see the
// fleet move, and says nothing a reader could authenticate with.
func TestHealthReportsTheCredentialWithoutNamingIt(t *testing.T) {
	path := credentialFile(t, testKeyID, testSecret)
	log := &capturedLog{}
	w := testWatcher(t, path, log.fn)

	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.Deps.Credentials = w
	sup.Deps.Log = log.fn
	sup.vol = healthyVolume()
	sup.deadline = NewDeadline(time.Now().Add(2*time.Minute), 45*time.Second, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sup.writeHealth()
	raw, err := os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		t.Fatalf("read health: %v", err)
	}
	if bytes.Contains(raw, []byte(testSecret)) || bytes.Contains(raw, []byte(testKeyID)) {
		t.Fatalf("health.json names the credential: %s", raw)
	}
	var h Health
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if h.CredentialRefreshFailed {
		t.Error("a healthy credential must not report a failed refresh")
	}
	if h.CredentialGeneration != 1 {
		t.Errorf("credential_generation = %d, want 1", h.CredentialGeneration)
	}

	// Now break the file and prove the warning surfaces.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w.Poll()
	sup.writeHealth()
	raw, err = os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		t.Fatalf("read health: %v", err)
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if !h.CredentialRefreshFailed {
		t.Fatalf("a failing refresh must reach health.json: %s", raw)
	}
	if h.CredentialGeneration != 1 {
		t.Errorf("a failed refresh must not advance the generation, got %d", h.CredentialGeneration)
	}
}

// TestTheCrashReportNamesNoCredential is the last reported surface: the JSON
// line the plugin republishes into a kubelet event.
func TestTheCrashReportNamesNoCredential(t *testing.T) {
	var buf bytes.Buffer
	WriteTerminal(&buf, "550e8400-e29b-41d4-a716-446655440000", 3, fatalf(
		CodeObjectStore, ErrCodeObjectStoreUnreachable, true,
		"object store refused this worker's credential for %s", CredentialRejectGrace))
	out := buf.String()
	if strings.Contains(out, testSecret) || strings.Contains(out, testKeyID) {
		t.Fatalf("the terminal report names the credential: %s", out)
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &line); err != nil {
		t.Fatalf("the terminal report must be one JSON object: %v (%s)", err, out)
	}
	for k, v := range line {
		if strings.Contains(strings.ToLower(k), "key") || strings.Contains(strings.ToLower(k), "secret") {
			t.Fatalf("the terminal report carries a %s field: %v", k, v)
		}
	}
}

// TestAWatcherlessSupervisorStillRuns keeps the credential tick optional, so
// every existing test in this package — and any future one that does not care
// about the object store — is unaffected.
func TestAWatcherlessSupervisorStillRuns(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	if sup.Deps.Credentials != nil {
		t.Fatal("the fixture must not install a watcher")
	}
	if got := sup.Deps.Credentials.Interval(); got != CredentialPollInterval {
		t.Fatalf("Interval on a nil watcher = %s, want %s", got, CredentialPollInterval)
	}
	if f := sup.pollCredential(context.Background()); f != nil {
		t.Fatalf("a supervisor without a watcher must not stop: %v", f.Err)
	}
}
