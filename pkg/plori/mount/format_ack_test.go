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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/mountspec"
)

// runToReady drives a whole Run that reaches the mount and is then asked to
// stop. The SIGTERM is queued before Run starts, so the run loop takes it on
// its first pass and the test does not depend on a sleep.
func runToReady(t *testing.T, sup *Supervisor) *Fatal {
	t.Helper()
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	return sup.Run(context.Background(), stop)
}

func readyExists(t *testing.T, sup *Supervisor) bool {
	t.Helper()
	_, err := os.Stat(sup.Paths.ReadyPath())
	return err == nil
}

// A first boot formats a filesystem the control-plane has never seen, and the
// control-plane learns what it is before the plugin is allowed to publish it.
// Without this call the volume stays `allocating` for its whole life: the Files
// router sees no active generation and serves the Agent's panel out of Orlop
// while the Agent's own filesystem is this mount (PLO-420).
func TestAFirstBootAcknowledgesTheFormatBeforeReady(t *testing.T) {
	spec := bootstrapSpec()
	vol := healthyVolume()
	cp := &fakeCP{}
	fs := &fakeFS{vol: vol}
	sup := newSup(t, spec, fs, cp, &fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})

	got := runToReady(t, sup)
	if got.Exit != CodeOK {
		t.Fatalf("exit = %d / %s, want a clean stop: %v", got.Exit, got.ErrCode, got.Err)
	}
	if !fs.formatted {
		t.Fatal("the bootstrap spec authorised a format and the empty replica needed one")
	}

	acks := cp.formatAcks()
	if len(acks) != 1 {
		t.Fatalf("format acks = %d, want exactly 1: %v", len(acks), cp.order())
	}
	ack := acks[0]
	if ack.uuid != vol.id.UUID {
		t.Errorf("acked uuid = %q, want the identity the mount proved, %q", ack.uuid, vol.id.UUID)
	}
	if ack.volume != spec.StorageVolumeID {
		t.Errorf("acked volume = %q, want %q", ack.volume, spec.StorageVolumeID)
	}
	if ack.epoch != spec.FenceEpoch {
		t.Errorf("acked epoch = %d, want the epoch this worker holds, %d", ack.epoch, spec.FenceEpoch)
	}
	// The plugin waits for the ready file and publishes the volume the moment it
	// appears. An ack that lands after it is an ack nothing waited for.
	if ack.readyExists {
		t.Error("the ready file already existed when the format was acknowledged")
	}
	if !readyExists(t, sup) {
		t.Error("an accepted ack must be followed by the ready file")
	}
	// The replica is seeded before the ack, not after: the control-plane must
	// not be told a filesystem exists before its metadata is recoverable.
	order := cp.order()
	if i, j := indexOf(order, "durable_point"), indexOf(order, "format_ack"); i < 0 || j < 0 || i > j {
		t.Errorf("call order = %v, want the seeded durable point before the format ack", order)
	}
}

// The trigger is the control-plane's `may_format`, not "this process formatted".
// A worker that formatted, seeded its replica and died before acking comes back
// to a replica that RESTORES and never formats again — and under a
// "formatted here" rule would never ack either, leaving the volume `allocating`
// for the rest of its life. That is the same split PLO-420 observed, merely
// narrower, so the same rule has to close it.
func TestAWorkerThatDiedBeforeAckingAcknowledgesOnTheNextBoot(t *testing.T) {
	spec := bootstrapSpec()
	vol := healthyVolume()
	cp := &fakeCP{}
	fs := &fakeFS{vol: vol}
	// No ErrReplicaEmpty: the previous generation's replica is there to restore.
	sup := newSup(t, spec, fs, cp, &fakeReplicator{}, &fakeFencer{})

	if got := runToReady(t, sup); got.Exit != CodeOK {
		t.Fatalf("exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if fs.formatted {
		t.Fatal("a replica that restored must never be formatted over")
	}
	acks := cp.formatAcks()
	if len(acks) != 1 {
		t.Fatalf("format acks = %d, want exactly 1: %v", len(acks), cp.order())
	}
	if acks[0].uuid != vol.id.UUID {
		t.Errorf("acked uuid = %q, want the restored filesystem's %q", acks[0].uuid, vol.id.UUID)
	}
}

// A volume the control-plane already has a Format.UUID for is never acked
// again. `may_format` is false on every publish after the first, and the ack is
// the one call on this surface that must not become a per-mount round trip.
func TestAVolumeTheControlPlaneAlreadyKnowsIsNotAcknowledged(t *testing.T) {
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, cp, &fakeReplicator{}, &fakeFencer{})

	if got := runToReady(t, sup); got.Exit != CodeOK {
		t.Fatalf("exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if acks := cp.formatAcks(); len(acks) != 0 {
		t.Fatalf("format acks = %d, want none on a volume that is already active: %v", len(acks), acks)
	}
}

// A refused ack means the control-plane does not know what filesystem this
// volume is. Serving it anyway is the split this call exists to prevent, so the
// mount is refused and the ready file is never written.
func TestARefusedFormatAcknowledgementRefusesTheMount(t *testing.T) {
	tests := map[string]struct {
		err      error
		exit     int
		errCode  string
		attempts int
		released string
	}{
		"a different filesystem already owns this prefix": {
			err:      &CPError{Status: http.StatusConflict, Code: CPCodeFormatMismatch, Msg: "another format uuid"},
			exit:     CodeIdentityMismatch,
			errCode:  ErrCodeIdentityMismatch,
			attempts: 1,
			released: "identity_mismatch",
		},
		"the token was rejected": {
			err:      &CPError{Status: http.StatusUnauthorized, Code: CPCodeTokenInvalid, Msg: "bad bearer token"},
			exit:     CodeIdentityMismatch,
			errCode:  ErrCodeIdentityMismatch,
			attempts: 1,
			released: "identity_mismatch",
		},
		"the control-plane never answered": {
			err:      &CPError{Status: http.StatusBadGateway, Code: CPCodeInternal, Msg: "upstream"},
			exit:     CodeIdentityMismatch,
			errCode:  ErrCodeIdentityMismatch,
			attempts: ackFormatAttempts,
			released: "identity_mismatch",
		},
		"the epoch was taken away": {
			err:      &CPError{Status: http.StatusConflict, Code: CPCodeStaleEpoch, Msg: "volume is at epoch 4"},
			exit:     CodeFenced,
			errCode:  ErrCodeFencedOutOfBand,
			attempts: 1,
			released: ReasonFencedOutOfBand,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cp := &fakeCP{ackErr: tc.err}
			sup := newSup(t, bootstrapSpec(), &fakeFS{vol: healthyVolume()}, cp,
				&fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})

			got := sup.Run(context.Background(), make(chan os.Signal))
			if got.Exit != tc.exit || got.ErrCode != tc.errCode {
				t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, tc.exit, tc.errCode, got.Err)
			}
			if readyExists(t, sup) {
				t.Error("a volume the control-plane does not recognise must never be published to the Agent")
			}
			if n := len(cp.formatAcks()); n != tc.attempts {
				t.Errorf("attempts = %d, want %d", n, tc.attempts)
			}
			// The lease goes back, or the Agent waits a whole TTL for a mount
			// that never was.
			if cp.released != tc.released {
				t.Errorf("release reason = %q, want %q", cp.released, tc.released)
			}
		})
	}
}

// A transport failure is not the control-plane's answer, so it is retried — and
// the retry is bounded, because "keep trying" and "serve it anyway" are the same
// outcome from the Agent's side.
func TestATransientAcknowledgementFailureIsRetriedAndThenSucceeds(t *testing.T) {
	cp := &flakyAckCP{fakeCP: fakeCP{}, failures: 2,
		err: &CPError{Status: http.StatusServiceUnavailable, Code: CPCodeInternal, Msg: "rolling"}}
	sup := newSup(t, bootstrapSpec(), &fakeFS{vol: healthyVolume()}, &cp.fakeCP,
		&fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})
	sup.Deps.CP = cp

	if got := runToReady(t, sup); got.Exit != CodeOK {
		t.Fatalf("exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if n := len(cp.formatAcks()); n != 3 {
		t.Fatalf("attempts = %d, want 3 (two refusals and the one that landed)", n)
	}
	if !readyExists(t, sup) {
		t.Error("an ack that eventually landed must still publish the volume")
	}
}

// flakyAckCP answers the first `failures` acks with err and then behaves.
type flakyAckCP struct {
	fakeCP
	failures int
	err      error
}

func (c *flakyAckCP) AckFormat(ctx context.Context, volumeID string, epoch int64, uuid string) (VolumeStateResponse, error) {
	c.mu.Lock()
	fail := c.failures > 0
	if fail {
		c.failures--
	}
	c.mu.Unlock()
	if fail {
		c.fakeCP.ackErr = c.err
	} else {
		c.fakeCP.ackErr = nil
	}
	return c.fakeCP.AckFormat(ctx, volumeID, epoch, uuid)
}

// ClientRoutes is a claim about what this client speaks, and plori-runtime's
// services/storage-worker compares it against the control-plane's own published
// surface. A claim that does not match the client is worse than no claim, so it
// is asserted against the paths the client actually posts to.
func TestTheClientSpeaksEveryRouteItDeclares(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewClient(srv.URL, tokenFile, 5*time.Second)
	ctx := context.Background()
	if _, err := c.RenewLease(ctx, "v", 1, RenewRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseLease(ctx, "v", 1, ReasonShutdown); err != nil {
		t.Fatal(err)
	}
	if err := c.ReportUsage(ctx, "v", 1, Usage{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := c.ReportDurablePoint(ctx, "v", 1, BarrierResult{}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AckFormat(ctx, "v", 1, "uuid"); err != nil {
		t.Fatal(err)
	}

	declared := append([]string(nil), ClientRoutes()...)
	sort.Strings(declared)
	sort.Strings(seen)
	if !slices.Equal(declared, seen) {
		t.Fatalf("ClientRoutes() = %v\nbut the client posted to %v\n"+
			"the declaration is what services/storage-worker compares against the "+
			"control-plane's surface; it has to be what this client actually speaks", declared, seen)
	}
	if slices.Contains(declared, mountspec.RouteMountSpec) {
		t.Error("/mount-spec is the CSI plugin's call; the worker receives its result in --spec-file")
	}
}

// The ack body is the control-plane's formatAckRequest, field for field. It is
// spelled out here because the request is built from a map literal, where a
// renamed key is not a compile error on either side.
func TestTheAcknowledgementCarriesTheControlPlanesFieldNames(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"storage_volume_id":"v","state":"active"}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := NewClient(srv.URL, tokenFile, 5*time.Second).
		AckFormat(context.Background(), "vol-1", 7, "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e")
	if err != nil {
		t.Fatal(err)
	}
	if res.State != VolumeStateActive {
		t.Errorf("state = %q, want the control-plane's answer decoded", res.State)
	}
	for key, want := range map[string]any{
		"volume_id":   "vol-1",
		"fence_epoch": float64(7),
		"format_uuid": "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e",
	} {
		if got, ok := body[key]; !ok || got != want {
			t.Errorf("body[%q] = %v (present %v), want %v", key, got, ok, want)
		}
	}
	if len(body) != 3 {
		t.Errorf("body = %v, want exactly the three fields formatAckRequest declares", body)
	}
}
