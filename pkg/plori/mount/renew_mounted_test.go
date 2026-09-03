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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// PLO-418: the renew that says `mounted`.
//
// The control-plane admits a bounded number of concurrent metadata restores
// (PLO-384) and had to free each slot on something the worker sends. It chose
// the first renew, which is right for the ordinary mount — the renew loop
// starts after AwaitMounted — and wrong for exactly one path: the same-holder
// fence-marker reclaim (PLO-323 F-6) renews DURING startup, before restoring,
// to prove to the control-plane that the 412 marker is its own. On the
// crash-and-replay path the slot therefore freed while the worker was still
// pulling LTX, and the next queued restore was admitted over the top of it.
//
// So the worker now says which of the two it is. These tests pin the three
// facts the control-plane's gate rests on: the reclaim renew does not claim a
// mount, the first renew after `ready` does, and every renew after that keeps
// saying so.

// renewRecorder wraps a control-plane double and keeps every RenewRequest.
// It exists because the close-out doubles (leaseAuthority/asHolder) model
// identity and epochs and deliberately ignore the request body, and widening
// them would change tests that are about something else.
type renewRecorder struct {
	ControlPlane
	mu     sync.Mutex
	renews []RenewRequest
}

func (r *renewRecorder) RenewLease(ctx context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	r.mu.Lock()
	r.renews = append(r.renews, req)
	r.mu.Unlock()
	return r.ControlPlane.RenewLease(ctx, volumeID, epoch, req)
}

func (r *renewRecorder) seen() []RenewRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RenewRequest(nil), r.renews...)
}

// TestTheMarkerReclaimRenewDoesNotSayMounted is the whole reason the flag
// exists. This is the crash-and-replay shape of
// TestTheSameHolderReclaimsItsOwnEpochAfterACrash: epoch 9 claimed its marker,
// replicated, and died; the kubelet republishes and the control-plane replays
// the same epoch to the same Pod, so the replacement meets its own marker,
// gets a 412, and proves ownership with a renew — before it has restored a
// single object.
//
// That renew must not free the restore slot, and the way it says so is by not
// carrying `mounted`. The renew that follows `ready` does.
func TestTheMarkerReclaimRenewDoesNotSayMounted(t *testing.T) {
	auth := newLeaseAuthority(9, 2*time.Minute)
	auth.assign(9, "pod-a")
	fencer := newSharedFencer()

	spec := specAtEpoch(9, 2*time.Minute)
	spec.LeaseRenewInterval = Duration(20 * time.Millisecond)
	root := "agents-meta/" + spec.StorageVolumeID + "/"
	fencer.populate(root + "g9/")

	marker, err := json.Marshal(FenceMarker{Volume: spec.StorageVolumeID, Epoch: 9, ClaimedAt: "2026-09-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fencer.Claim(context.Background(), spec.FenceMarkerKey, marker); err != nil {
		t.Fatalf("seed the predecessor's marker: %v", err)
	}

	cp := &renewRecorder{ControlPlane: asHolder{auth, "pod-a"}}
	vol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 9}
	sup := newCloseoutSup(t, spec, vol, cp, &countingReplicator{}, fencer)

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	// Two renews: the reclaim's, and at least one from the loop after `ready`.
	waitFor(t, 10*time.Second, func() bool { return len(cp.seen()) >= 2 }, "timed out waiting for the reclaim renew and a renew from the loop")
	stop <- syscall.SIGTERM
	if f := waitFatal(t, done, 10*time.Second, "the supervisor did not stop"); f.Exit != CodeOK {
		t.Fatalf("the reclaiming holder exited %d (%v)", f.Exit, f.Err)
	}

	renews := cp.seen()
	if renews[0].Mounted {
		t.Error("the marker-reclaim renew claimed the volume was mounted; it runs BEFORE the restore, " +
			"and the control-plane frees the restore-admission slot on that claim (PLO-418)")
	}
	if !renews[1].Mounted {
		t.Error("the first renew after `ready` did not say mounted; the restore slot would then be held " +
			"until the lease deadline instead of the ~11 s the restore actually took")
	}
	for i, r := range renews[1:] {
		if !r.Mounted {
			t.Errorf("renew %d after ready stopped saying mounted; the flag is a state, not an edge", i+1)
		}
	}
}

// TestEveryRenewAfterReadySaysMounted is the ordinary mount, where there is no
// reclaim and the first renew is already the post-ready one. It also pins the
// idempotence: the control-plane may lose the renew that carried the edge — a
// 500 from a rolling replica, a timeout — and the next one has to be able to
// free the slot on its own.
func TestEveryRenewAfterReadySaysMounted(t *testing.T) {
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, cp, &fakeReplicator{}, &fakeFencer{})

	runUntil(t, sup, "three renews", func() bool { return len(cp.renewRequests()) >= 3 })

	renews := cp.renewRequests()
	for i, r := range renews {
		if !r.Mounted {
			t.Errorf("renew %d of an ordinary mount did not say mounted; every renew here is after `ready`", i)
		}
	}
}

// TestTheRenewBodyOmitsMountedUntilTheMountIsUp is the wire half. A pre-ready
// renew has to be byte-identical to the renew of a worker built before this
// field existed, because the control-plane reads both the same way — absent
// means "not mounted yet", keep the slot. Sending `"mounted": false` would be
// the same decision spelled twice, and would make an old worker and a
// restoring one distinguishable for no gain.
func TestTheRenewBodyOmitsMountedUntilTheMountIsUp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		req     RenewRequest
		present bool
	}{
		{"restoring", RenewRequest{}, false},
		{"mounted", RenewRequest{Mounted: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := captureRenewBody(t, tc.req)
			got, ok := body["mounted"]
			if ok != tc.present {
				t.Fatalf("mounted present = %v, want %v (body %v)", ok, tc.present, body)
			}
			if ok && got != true {
				t.Errorf("mounted = %v, want true — the field is only ever sent to assert a mount", got)
			}
		})
	}
}

// captureRenewBody posts one RenewLease at a fake control-plane and hands back
// the decoded request body.
func captureRenewBody(t *testing.T, req RenewRequest) map[string]any {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/storage/lease/renew" {
			t.Errorf("posted to %s", r.URL.Path)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %s", err)
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("decode body: %s", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"storage_volume_id":"v-1","fence_epoch":7}`))
	}))
	t.Cleanup(srv.Close)

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("t"), 0o600); err != nil {
		t.Fatalf("write token: %s", err)
	}
	c := NewClient(srv.URL, tokenFile, 5*time.Second)
	if _, err := c.RenewLease(context.Background(), "v-1", 7, req); err != nil {
		t.Fatalf("renew: %s", err)
	}
	return got
}
