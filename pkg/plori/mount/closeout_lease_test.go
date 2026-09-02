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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// newCloseoutSup is newSup with the dependency set widened to the interfaces.
// The existing helper is typed to the concrete fakes in supervisor_test.go and
// these tests need to swap in a shared control-plane and a recording
// replicator, so they build the Supervisor here rather than changing a helper
// other tests depend on.
func newCloseoutSup(t *testing.T, spec *MountSpec, vol Volume, cp ControlPlane, rep Replicator, fencer Fencer) *Supervisor {
	t.Helper()
	dir := t.TempDir()
	return &Supervisor{
		Spec: spec,
		Paths: Paths{
			StateDir:   filepath.Join(dir, "state"),
			CacheDir:   filepath.Join(dir, "cache"),
			MountPoint: filepath.Join(dir, "mnt"),
		},
		Options: MountOptions{BarrierInterval: 30 * time.Millisecond},
		Deps: Deps{
			FS:                   &staticFS{vol: vol},
			CP:                   cp,
			Replicator:           rep,
			Fencer:               fencer,
			ControlGateInstalled: func() bool { return true },
		},
	}
}

// staticFS hands back one already-built Volume, whatever its concrete type.
type staticFS struct {
	vol       Volume
	formatted bool
}

func (f *staticFS) Format(context.Context, *MountSpec) error { f.formatted = true; return nil }
func (f *staticFS) Open(context.Context, *MountSpec) (Volume, error) {
	return f.vol, nil
}

// PLO-323 acceptance close-out: the concurrency and stale-holder bullets.
//
// The issue asks for "a deterministic concurrency test [that] never produces
// two writable mounts or two authoritative replica lineages". These tests run
// two real supervisors against one shared object store and one shared lease
// authority, and record exactly which of them was writable when.

// ---------------------------------------------------------------- doubles ---

// sharedFencer is one object store's fence-marker namespace with the real
// conditional-PUT semantics: If-None-Match: * succeeds exactly once per key.
// It is what makes the same-epoch race decidable without a bucket.
type sharedFencer struct {
	mu      sync.Mutex
	claimed map[string]int
	// bodies is what each winning claim actually PUT, so a second worker's
	// ReadMarker sees the real marker rather than a fixture.
	bodies map[string][]byte
	// populated names the generation prefixes that hold an LTX history.
	populated map[string]bool
	prior     string
}

func newSharedFencer() *sharedFencer {
	return &sharedFencer{claimed: map[string]int{}, bodies: map[string][]byte{}}
}

func (f *sharedFencer) Claim(_ context.Context, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimed[key]++
	if f.claimed[key] > 1 {
		return ErrFenceMarkerHeld
	}
	f.bodies[key] = append([]byte(nil), body...)
	return nil
}

func (f *sharedFencer) ReadMarker(_ context.Context, key string) (FenceMarker, error) {
	f.mu.Lock()
	body, ok := f.bodies[key]
	f.mu.Unlock()
	if !ok {
		return FenceMarker{}, ErrFenceMarkerMissing
	}
	var m FenceMarker
	if err := json.Unmarshal(body, &m); err != nil {
		return FenceMarker{}, err
	}
	return m, nil
}

// populate marks a generation prefix as holding more than its own fence
// marker, which is what prefixHasReplica means by "populated".
func (f *sharedFencer) populate(prefix string) {
	f.mu.Lock()
	if f.populated == nil {
		f.populated = map[string]bool{}
	}
	f.populated[prefix] = true
	f.mu.Unlock()
}

// PriorMetaPrefix runs the REAL candidate ordering over the prefixes this store
// holds, so the "at or below the epoch" rule is exercised rather than stubbed
// (PLO-323 F-6c). `prior` stays the shortcut for the tests that only care that
// some source was chosen.
func (f *sharedFencer) PriorMetaPrefix(_ context.Context, root string, epoch int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.populated) == 0 {
		return f.prior, nil
	}
	prefixes := make([]string, 0, len(f.populated))
	for p := range f.populated {
		prefixes = append(prefixes, p)
	}
	for _, c := range priorPrefixCandidates(prefixes, root, epoch) {
		if f.populated[c] {
			return c, nil
		}
	}
	return "", nil
}

// leaseAuthority models services/control-plane/internal/storagevol's lease
// table: one volume, one current epoch, monotonically increasing. A renew from
// any epoch below the current one is stale_epoch, which mountspec.md and
// CPError.Fenced() both make terminal.
type leaseAuthority struct {
	mu      sync.Mutex
	current int64
	ttl     time.Duration
	// writable records, per epoch, the instants at which that writer became
	// ready and at which it fenced itself. Overlap between two epochs is
	// exactly "two writable mounts".
	ready  map[int64]time.Time
	fenced map[int64]time.Time
	// released records every terminal reason handed back per epoch. Two
	// workers can share one epoch (the same-epoch race), so this is a list.
	released map[int64][]string
	// holder is which Pod the authority handed each epoch to. The real
	// control-plane never issues one epoch to two Pods — AcquireLease mints a
	// new epoch for a new holder and replays only for the same one — and the
	// renew route refuses any caller whose Pod is not the recorded holder
	// (storagespec/issuer.go authorizeHolder). Modelling that is what makes
	// the same-epoch reclaim decidable: without it every caller looks like the
	// owner, and F-6's idempotent claim would wave a stranger through.
	holder map[int64]string
}

func newLeaseAuthority(current int64, ttl time.Duration) *leaseAuthority {
	return &leaseAuthority{
		current:  current,
		ttl:      ttl,
		ready:    map[int64]time.Time{},
		fenced:   map[int64]time.Time{},
		released: map[int64][]string{},
		holder:   map[int64]string{},
	}
}

// assign records which Pod this epoch was issued to.
func (a *leaseAuthority) assign(epoch int64, pod string) {
	a.mu.Lock()
	a.holder[epoch] = pod
	a.mu.Unlock()
}

// asHolder is one Pod's view of the authority. On the real routes the Pod's
// identity travels in the projected ServiceAccount token, never in the request
// body, which is why the worker's ControlPlane calls carry no holder argument —
// so the double binds the identity the same way, by construction.
type asHolder struct {
	*leaseAuthority
	pod string
}

func (h asHolder) RenewLease(_ context.Context, _ string, epoch int64, _ RenewRequest) (LeaseResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if epoch < h.current {
		return LeaseResponse{}, &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "the presented epoch was moved past"}
	}
	if owner, ok := h.holder[epoch]; ok && owner != h.pod {
		return LeaseResponse{}, &CPError{
			Status: 403, Code: CPCodeIdentityMismatch,
			Msg: "this volume is not held by this pod",
		}
	}
	return LeaseResponse{FenceEpoch: epoch, LeaseExpiresAt: time.Now().UTC().Add(h.ttl)}, nil
}

// promote is the control-plane handing the volume to a new writer.
func (a *leaseAuthority) promote(epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch > a.current {
		a.current = epoch
	}
}

func (a *leaseAuthority) markReady(epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.ready[epoch]; !ok {
		a.ready[epoch] = time.Now()
	}
}

func (a *leaseAuthority) markFenced(epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.fenced[epoch]; !ok {
		a.fenced[epoch] = time.Now()
	}
}

func (a *leaseAuthority) RenewLease(_ context.Context, _ string, epoch int64, _ RenewRequest) (LeaseResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch < a.current {
		return LeaseResponse{}, &CPError{
			Status: 409,
			Code:   CPCodeStaleEpoch,
			Msg:    "the presented epoch was moved past",
		}
	}
	return LeaseResponse{FenceEpoch: epoch, LeaseExpiresAt: time.Now().UTC().Add(a.ttl)}, nil
}

func (a *leaseAuthority) ReleaseLease(_ context.Context, _ string, epoch int64, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.released[epoch] = append(a.released[epoch], reason)
	return nil
}

func (a *leaseAuthority) ReportUsage(context.Context, string, int64, Usage, time.Time) error {
	return nil
}

func (a *leaseAuthority) ReportDurablePoint(context.Context, string, int64, BarrierResult, string) error {
	return nil
}

// releasedWith reports whether any worker on this epoch handed the lease back
// with the given reason.
func (a *leaseAuthority) releasedWith(epoch int64, reason string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range a.released[epoch] {
		if r == reason {
			return true
		}
	}
	return false
}

func (a *leaseAuthority) reasons(epoch int64) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.released[epoch]...)
}

// observedOverlap is how long two epochs were simultaneously writable: from
// when the later one became ready until the earlier one fenced itself.
func (a *leaseAuthority) observedOverlap(older, newer int64) (time.Duration, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	newerReady, ok := a.ready[newer]
	if !ok {
		return 0, false
	}
	olderFenced, ok := a.fenced[older]
	if !ok {
		return 0, false
	}
	if !olderFenced.After(newerReady) {
		return 0, true
	}
	return olderFenced.Sub(newerReady), true
}

// countingReplicator records every upload the worker attempts, and when.
type countingReplicator struct {
	mu     sync.Mutex
	events []replicaEvent
	prior  string
}

type replicaEvent struct {
	op string
	at time.Time
}

func (r *countingReplicator) record(op string) {
	r.mu.Lock()
	r.events = append(r.events, replicaEvent{op: op, at: time.Now()})
	r.mu.Unlock()
}

func (r *countingReplicator) Restore(_ context.Context, src string, _ RestoreOptions) error {
	r.mu.Lock()
	r.prior = src
	r.mu.Unlock()
	r.record("restore")
	return nil
}
func (r *countingReplicator) Start(context.Context) error       { r.record("start"); return nil }
func (r *countingReplicator) SyncAndWait(context.Context) error { r.record("sync"); return nil }
func (r *countingReplicator) TxID(context.Context) (string, error) {
	return "0000000000000011", nil
}
func (r *countingReplicator) Stop(context.Context) error  { r.record("stop"); return nil }
func (r *countingReplicator) Abort(context.Context) error { r.record("abort"); return nil }

func (r *countingReplicator) restoredFrom() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prior
}

func (r *countingReplicator) has(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.op == op {
			return true
		}
	}
	return false
}

func (r *countingReplicator) uploadsAfter(t time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	for _, e := range r.events {
		if (e.op == "sync" || e.op == "stop") && e.at.After(t) {
			n++
		}
	}
	return n
}

// readyReportingVolume tells the lease authority when this epoch actually
// became writable and when it stopped being writable.
type readyReportingVolume struct {
	*fakeVolume
	auth  *leaseAuthority
	epoch int64
}

func (v *readyReportingVolume) AwaitMounted(ctx context.Context) error {
	if err := v.fakeVolume.AwaitMounted(ctx); err != nil {
		return err
	}
	v.auth.markReady(v.epoch)
	return nil
}

func (v *readyReportingVolume) FenceWrites() {
	v.fakeVolume.FenceWrites()
	v.auth.markFenced(v.epoch)
}

// specAtEpoch builds the spec the control-plane would issue for one writer
// epoch. The metadata prefix and the fence-marker key are partitioned by epoch
// (control-plane-model.md MetaPrefix/FenceMarkerKey); the data prefix is not.
// Getting that partitioning right in the double is what makes the two-epoch
// test mean anything: with one shared marker key the second epoch would lose
// the conditional PUT for the wrong reason.
func specAtEpoch(epoch int64, ttl time.Duration) *MountSpec {
	spec := testSpec()
	spec.FenceEpoch = epoch
	spec.LeaseExpiresAt = time.Now().UTC().Add(ttl)
	root := "agents-meta/" + spec.StorageVolumeID + "/"
	spec.MetaPrefix = fmt.Sprintf("%sg%d/", root, epoch)
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	return spec
}

// ------------------------------------------------------------------ tests ---

// The same-epoch half of the concurrency acceptance, split in two because the
// answer is not the same for the two workers that can present one epoch.
//
// The fence marker is a conditional PUT on one key, so of two workers handed
// epoch N exactly one wins the PUT. What the loser does with its 412 is the
// whole of PLO-323 F-6: before the fix it was always terminal, which made an
// ordinary worker crash cost the Agent a full lease TTL of downtime.

// TestASecondHolderAtTheSameEpochNeverMounts is the case that must stay
// fail-closed. The control-plane never hands one epoch to two Pods, so the only
// way a second holder presents epoch N is a replayed spec file — and that one
// must still lose, because its writes would land in a metadata prefix and a
// shared data prefix the real holder owns.
func TestASecondHolderAtTheSameEpochNeverMounts(t *testing.T) {
	auth := newLeaseAuthority(9, 2*time.Minute)
	auth.assign(9, "pod-a")
	fencer := newSharedFencer()

	// The real holder mounts epoch 9 and stops cleanly. Nothing deletes a fence
	// marker, so its claim outlives it — which is exactly what the stranger
	// walks into.
	specA := specAtEpoch(9, 2*time.Minute)
	volA := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 9}
	supA := newCloseoutSup(t, specA, volA, asHolder{auth, "pod-a"}, &countingReplicator{}, fencer)
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	if f := supA.Run(context.Background(), stop); f.Exit != CodeOK {
		t.Fatalf("the holder of epoch 9 exited %d: %v", f.Exit, f.Err)
	}

	// A different Pod arrives holding a copy of the same spec.
	specB := specAtEpoch(9, 2*time.Minute)
	volB := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 9}
	repB := &countingReplicator{}
	supB := newCloseoutSup(t, specB, volB, asHolder{auth, "pod-b"}, repB, fencer)

	f := supB.Run(context.Background(), make(chan os.Signal))
	if f.Exit != CodeFenced {
		t.Fatalf("the stranger exited %d (%v), want %d", f.Exit, f.Err, CodeFenced)
	}
	if f.ErrCode != ErrCodeFenceMarkerHeld {
		t.Errorf("stranger error code = %s, want %s", f.ErrCode, ErrCodeFenceMarkerHeld)
	}
	// It must not have restored or replicated anything: the marker is settled
	// before the first LTX read or write.
	if n := len(repB.events); n != 0 {
		t.Errorf("the fenced-out worker touched the replica %d times, want 0: %v", n, repB.events)
	}
	if !auth.releasedWith(9, ReasonFencedOutOfBand) {
		t.Errorf("the stranger released epoch 9 with %q, want %q", auth.reasons(9), ReasonFencedOutOfBand)
	}
}

// TestTheSameHolderReclaimsItsOwnEpochAfterACrash is the case PLO-323 F-6
// reopened the issue for, and it is the ordinary one: the worker crashes,
// nothing releases the lease — nothing else may — the kubelet retries
// NodePublish, and the control-plane replays the SAME epoch for the same Pod.
// The replacement then meets its own predecessor's fence marker.
//
// Two things must hold. It must mount at all: before the fix the 412 was
// terminal and the volume stayed unmountable for the rest of the lease TTL. And
// it must restore from epoch 9's OWN prefix — the crash happened before the
// first durable-point post, so the control-plane names no source, and the
// listing fallback used to look strictly BELOW the epoch and hand back epoch
// 8's replica, silently dropping everything epoch 9 had written (F-6c).
func TestTheSameHolderReclaimsItsOwnEpochAfterACrash(t *testing.T) {
	auth := newLeaseAuthority(9, 2*time.Minute)
	auth.assign(9, "pod-a")
	fencer := newSharedFencer()

	spec := specAtEpoch(9, 2*time.Minute)
	root := "agents-meta/" + spec.StorageVolumeID + "/"

	// The store as the crashed generation left it: epoch 8 replicated and
	// finished, epoch 9 claimed its marker and replicated, then the process
	// died before it could post a durable point.
	fencer.populate(root + "g8/")
	fencer.populate(root + "g9/")
	marker, err := json.Marshal(FenceMarker{Volume: spec.StorageVolumeID, Epoch: 9, ClaimedAt: "2026-09-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fencer.Claim(context.Background(), spec.FenceMarkerKey, marker); err != nil {
		t.Fatalf("seed the predecessor's marker: %v", err)
	}

	vol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 9}
	rep := &countingReplicator{}
	sup := newCloseoutSup(t, spec, vol, asHolder{auth, "pod-a"}, rep, fencer)
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM

	f := sup.Run(context.Background(), stop)
	if f.Exit != CodeOK {
		t.Fatalf("the restarted holder exited %d (%v); its own epoch must be reclaimable", f.Exit, f.Err)
	}
	if got := rep.restoredFrom(); got != spec.MetaPrefix {
		t.Errorf("restored from %q, want epoch 9's own prefix %q — anything else drops epoch 9's writes",
			got, spec.MetaPrefix)
	}
}

// TestASupersededWriterFencesItselfWithinOneRenewInterval is the other half,
// and it is the honest one: across two DIFFERENT epochs the worker cannot be
// the fence. Each epoch claims its own marker key, so both mounts succeed, and
// the older writer only learns it lost the volume on its next renew.
//
// So the guarantee is not "never two writable mounts" at this layer — it is
// "at most one renew interval of overlap, and only if the control-plane
// promoted a new epoch while the old lease was still live" (an admin revoke or
// a control-plane failover; a normal handover waits for the TTL because
// storagevol.AcquireLease refuses with lease_held while the incumbent is
// alive). This test measures that window and pins the bound.
func TestASupersededWriterFencesItselfWithinOneRenewInterval(t *testing.T) {
	const renew = 40 * time.Millisecond
	auth := newLeaseAuthority(11, 2*time.Minute)
	fencer := newSharedFencer()

	oldSpec := specAtEpoch(11, 2*time.Minute)
	oldSpec.LeaseRenewInterval = Duration(renew)
	oldVol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 11}
	oldRep := &countingReplicator{}
	oldSup := newCloseoutSup(t, oldSpec, oldVol, auth, oldRep, fencer)

	done := make(chan *Fatal, 1)
	go func() { done <- oldSup.Run(context.Background(), make(chan os.Signal)) }()

	// Wait until the incumbent is actually serving before promoting.
	waitFor(t, time.Second, func() bool {
		auth.mu.Lock()
		defer auth.mu.Unlock()
		_, ok := auth.ready[11]
		return ok
	}, "incumbent never became ready")

	// The control-plane hands the volume to epoch 12 while 11's lease is still
	// live. This is the forced-admin-revoke fault from the issue.
	auth.promote(12)
	promotedAt := time.Now()

	newSpec := specAtEpoch(12, 2*time.Minute)
	newSpec.LeaseRenewInterval = Duration(renew)
	newVol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 12}
	newRep := &countingReplicator{}
	newSup := newCloseoutSup(t, newSpec, newVol, auth, newRep, fencer)
	newStop := make(chan os.Signal, 1)
	newDone := make(chan *Fatal, 1)
	go func() { newDone <- newSup.Run(context.Background(), newStop) }()

	oldFatal := waitFatal(t, done, 3*time.Second, "the superseded writer never exited")
	if oldFatal.Exit != CodeFenced {
		t.Fatalf("superseded writer exit = %d (%v), want %d", oldFatal.Exit, oldFatal.Err, CodeFenced)
	}
	// stale_epoch is the out-of-band fence: the epoch was taken away rather
	// than allowed to run out, so the stop skips the barrier and the final
	// sync (PLO-323 F-1) and says so in its typed identifier.
	if oldFatal.ErrCode != ErrCodeFencedOutOfBand {
		t.Errorf("superseded writer error code = %s, want %s", oldFatal.ErrCode, ErrCodeFencedOutOfBand)
	}
	if !oldVol.Fenced() {
		t.Error("the superseded writer exited 66 without fencing its own filesystem")
	}
	if !auth.releasedWith(11, ReasonFencedOutOfBand) {
		t.Errorf("superseded writer released with %q, want %q", auth.reasons(11), ReasonFencedOutOfBand)
	}

	// The bound: the incumbent must notice within one renew interval of the
	// promotion, plus scheduling slack.
	if elapsed := time.Since(promotedAt); elapsed > renew+2*time.Second {
		t.Errorf("superseded writer took %s to fence itself; the bound is one renew interval (%s)", elapsed, renew)
	}
	if overlap, ok := auth.observedOverlap(11, 12); ok && overlap > renew+time.Second {
		t.Errorf("two writable mounts overlapped for %s, want at most one renew interval (%s)", overlap, renew)
	}

	newStop <- syscall.SIGTERM
	if f := waitFatal(t, newDone, 3*time.Second, "the new writer never exited"); f.Exit != CodeOK {
		t.Errorf("new writer exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
}

// TestAnOutOfBandFenceUploadsNothingOnItsWayOut is the F-1 ruling, asserted.
//
// The threat model states the drain rule entirely in terms of expiry
// (threat-model.md §7.5) and, before this, said nothing about a writer fenced
// OUT OF BAND — a 412 on the marker, or stale_epoch/lease_held from a renew.
// The implementation resolved that silence in the less safe direction: it ran
// the full ordered stop, spending whatever its LOCAL deadline still said it had
// on a barrier that flushes the writeback cache into the SHARED data prefix,
// and on LTX pushes into the metadata prefix its successor restores from.
//
// The ruling (PLO-323, recorded in threat-model.md §7): an out-of-band fence
// skips the remote barrier and the final sync. It seals, detaches without a
// flush, closes, reports and releases. Nothing it staged reaches the store,
// because no barrier ran to make that history's blocks durable — a successor
// restoring it would find metadata referencing objects that were never
// uploaded.
func TestAnOutOfBandFenceUploadsNothingOnItsWayOut(t *testing.T) {
	auth := newLeaseAuthority(20, 2*time.Minute)
	spec := specAtEpoch(20, 2*time.Minute)
	spec.LeaseRenewInterval = Duration(30 * time.Millisecond)
	vol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 20}
	rep := &countingReplicator{}
	sup := newCloseoutSup(t, spec, vol, auth, rep, newSharedFencer())

	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	waitFor(t, time.Second, func() bool {
		auth.mu.Lock()
		defer auth.mu.Unlock()
		_, ok := auth.ready[20]
		return ok
	}, "writer never became ready")

	// Revoke the epoch while the local lease still has ~2 minutes on it: the
	// worker's own deadline would happily fund a full barrier here, which is
	// what makes this about authority rather than about time.
	auth.promote(21)
	revokedAt := time.Now()

	f := waitFatal(t, done, 3*time.Second, "the revoked writer never exited")
	if f.Exit != CodeFenced {
		t.Fatalf("exit = %d (%v), want %d", f.Exit, f.Err, CodeFenced)
	}
	if f.ErrCode != ErrCodeFencedOutOfBand {
		t.Errorf("error code = %s, want %s", f.ErrCode, ErrCodeFencedOutOfBand)
	}

	// No barrier after the fence, and no upload of any kind after revocation.
	order := vol.order()
	for i, c := range order {
		if c == "fence" {
			for _, later := range order[i:] {
				if later == "barrier" {
					t.Errorf("a durability barrier ran after the fence: %v", order)
				}
				if later == "unmount" {
					t.Errorf("the mount was flushed on its way out; an out-of-band fence detaches: %v", order)
				}
			}
			break
		}
	}
	if !contains(order, "detach") {
		t.Errorf("volume call order %v: the mount must be detached without a flush", order)
	}
	if n := rep.uploadsAfter(revokedAt); n != 0 {
		t.Errorf("%d replica upload(s) after revocation, want 0: %v", n, rep.events)
	}
	if !rep.has("abort") {
		t.Errorf("replication was not aborted: %v; a graceful stop performs its own final sync", rep.events)
	}
	if !auth.releasedWith(20, ReasonFencedOutOfBand) {
		t.Errorf("released with %q, want %q", auth.reasons(20), ReasonFencedOutOfBand)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// TestADeadlineTripKeepsItsBoundedFlush is the other side of the same ruling:
// when the lease was not taken away but simply ran down, the ordered stop is
// still the right answer. threat-model.md §7.5 rejects both "fence then flush"
// and "flush then fence" and requires a bounded flush window INSIDE the lease,
// with an incomplete flush reported as data loss. So a deadline trip must still
// run its barrier and its final sync — bounded by what is left of the lease.
func TestADeadlineTripKeepsItsBoundedFlush(t *testing.T) {
	spec := testSpec()
	// Already inside the write-stop margin when the worker starts, with the
	// control-plane unreachable so no renewal can move the deadline.
	spec.LeaseExpiresAt = time.Now().UTC().Add(2 * time.Second)
	spec.WriteStopMargin = Duration(1900 * time.Millisecond)
	spec.LeaseRenewInterval = Duration(30 * time.Millisecond)

	vol := healthyVolume()
	rep := &countingReplicator{}
	sup := newCloseoutSup(t, spec, vol, &fakeCP{renewErr: context.DeadlineExceeded}, rep, newSharedFencer())

	f := sup.Run(context.Background(), make(chan os.Signal))
	if f.Exit != CodeFenced && f.Exit != CodeBarrierIncomplete {
		t.Fatalf("exit = %d (%v), want 66 or 69", f.Exit, f.Err)
	}
	order := vol.order()
	if !contains(order, "barrier") {
		t.Errorf("volume call order %v: the deadline path keeps its bounded flush", order)
	}
	if !contains(order, "unmount") {
		t.Errorf("volume call order %v: the deadline path unmounts with a flush", order)
	}
	if contains(order, "detach") {
		t.Errorf("volume call order %v: only an out-of-band fence detaches without flushing", order)
	}
	if !rep.has("sync") {
		t.Errorf("the deadline path skipped its final replica sync: %v", rep.events)
	}
	if rep.has("abort") {
		t.Errorf("the deadline path killed replication instead of stopping it: %v", rep.events)
	}
}

// TestAnUnreachableControlPlaneFencesWithoutUploadingPastTheDeadline is the
// other direction of the same bullet, and here the behaviour is right: when
// renewals simply stop arriving, the deadline guard trips at the write-stop
// margin and the shutdown budget is the one-second floor, so the worker cannot
// spend a whole TTL uploading.
func TestAnUnreachableControlPlaneFencesWithoutUploadingPastTheDeadline(t *testing.T) {
	spec := testSpec()
	// The lease is already inside its write-stop margin when the worker starts.
	spec.LeaseExpiresAt = time.Now().UTC().Add(200 * time.Millisecond)
	spec.WriteStopMargin = Duration(150 * time.Millisecond)
	spec.LeaseRenewInterval = Duration(30 * time.Millisecond)

	cp := &fakeCP{renewErr: context.DeadlineExceeded}
	vol := healthyVolume()
	rep := &countingReplicator{}
	sup := newCloseoutSup(t, spec, vol, cp, rep, newSharedFencer())

	started := time.Now()
	f := sup.Run(context.Background(), make(chan os.Signal))
	if f.Exit != CodeFenced && f.Exit != CodeBarrierIncomplete {
		t.Fatalf("exit = %d (%v), want 66 or 69", f.Exit, f.Err)
	}
	if !vol.Fenced() {
		t.Error("the worker exited without fencing its filesystem")
	}
	// It must not have sat there for a full TTL: the deadline guard runs every
	// second and the shutdown is bounded by what is left of the lease.
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("took %s to fence on an unreachable control-plane", elapsed)
	}
	if cp.released == "" {
		t.Error("the lease was never released")
	}
}

// --------------------------------------------------------------- helpers ---

func waitFor(t *testing.T, limit time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func waitFatal(t *testing.T, ch <-chan *Fatal, limit time.Duration, msg string) *Fatal {
	t.Helper()
	select {
	case f := <-ch:
		return f
	case <-time.After(limit):
		t.Fatal(msg)
		return nil
	}
}

// TestALateRenewResponseCannotExtendTheLeasePastItsIssue is PLO-323's "delayed
// response arriving after expiry" fault.
//
// Deadline.Update anchors on the expiry the SERVER stated and only re-expresses
// it in this process's monotonic clock (lease.go:74-81): `remaining` shrinks by
// exactly as much as `now` advanced, so the instant the deadline lands on does
// not move. A response that spent 30 s in flight therefore buys no extra write
// window — which is the property that matters, because the unsafe alternative
// (treating the answer as "a fresh TTL starting now") would hand a worker a
// full lease term every time the control-plane was slow.
func TestALateRenewResponseCannotExtendTheLeasePastItsIssue(t *testing.T) {
	const (
		margin = 45 * time.Second
		term   = 2 * time.Minute
	)
	sentAt := time.Now()
	// The control-plane issues a two-minute lease measured from when it saw
	// the request.
	serverExpiry := sentAt.UTC().Add(term)

	prompt := NewDeadline(serverExpiry, margin, sentAt)
	// The same answer, converted 30 s later because the response crawled back.
	delay := 30 * time.Second
	late := NewDeadline(serverExpiry, margin, sentAt.Add(delay))

	// Measured from one common instant, both deadlines are the same: the late
	// conversion did not move the write-stop margin forward.
	at := sentAt.Add(time.Minute)
	promptRemaining := prompt.Remaining(at)
	lateRemaining := late.Remaining(at)
	if drift := lateRemaining - promptRemaining; drift > time.Millisecond || drift < -time.Millisecond {
		t.Fatalf("a %s-late renew moved the write-stop margin by %s; it must not move at all",
			delay, drift)
	}

	// And the unsafe shape is absent: the deadline tracks the server's expiry,
	// not "arrival plus a term". If it were the latter, a worker would still be
	// writing `delay` past the lease it actually holds.
	if late.WriteAllowed(sentAt.Add(term - margin + time.Second)) {
		t.Error("writes are still allowed past the server's expiry minus the margin")
	}
	if !late.Expired(sentAt.Add(term + time.Second)) {
		t.Error("the lease outlived the server's own deadline after a delayed response")
	}
	// A response that arrives after the lease already died leaves the worker
	// fenced rather than reviving it.
	dead := NewDeadline(sentAt.UTC().Add(-time.Second), margin, sentAt)
	if dead.WriteAllowed(sentAt) || !dead.Expired(sentAt) {
		t.Error("an already-expired expiry was accepted as a live lease")
	}
}

// TestTheLeaseExpiryReachesTheMetadataEngine is PLO-323 F-5 at the seam that
// carries it. threat-model.md:812-815 requires the deadline to be re-checked
// "immediately before every write submission, not on a timer", and lease.go
// repeated the requirement almost word for word — while the only caller of the
// check was a one-second ticker, which is the timer the requirement forbids.
//
// The fix moved the check into the metadata engine, so this asserts the one
// thing the supervisor still owns: the engine is armed before the loop starts
// and re-armed after every renewal, with the LEASE EXPIRY rather than the
// write-stop margin. The margin is the tail of the lease reserved for the flush
// and the barrier (lease.go); arming the engine with it would make the bounded
// flush window threat-model.md §7.5 mandates impossible.
func TestTheLeaseExpiryReachesTheMetadataEngine(t *testing.T) {
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(20 * time.Millisecond)
	vol := healthyVolume()
	renewed := time.Now().UTC().Add(90 * time.Second)
	cp := &fakeCP{expiry: func() time.Time { return renewed }}
	sup := newCloseoutSup(t, spec, vol, cp, &countingReplicator{}, newSharedFencer())

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	// Armed from the renewal, not from the spec the worker started with.
	waitFor(t, 2*time.Second, func() bool {
		vol.mu.Lock()
		defer vol.mu.Unlock()
		return !vol.writeExpiry.IsZero() && vol.writeExpiry.After(time.Now().Add(60*time.Second))
	}, "the metadata engine was never armed with the renewed expiry")

	vol.mu.Lock()
	armed := vol.writeExpiry
	vol.mu.Unlock()
	if margin := sup.deadline.Expiry().Add(-spec.WriteStopMargin.D()); !armed.After(margin) {
		t.Errorf("engine armed at %s, which is at or before the write-stop margin %s; "+
			"the margin is reserved for the drain, not for sealing", armed, margin)
	}

	stop <- syscall.SIGTERM
	if f := waitFatal(t, done, 3*time.Second, "the worker never stopped"); f.Exit != CodeOK {
		t.Fatalf("exit = %d: %v", f.Exit, f.Err)
	}
}
