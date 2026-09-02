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
	prior   string
}

func newSharedFencer() *sharedFencer {
	return &sharedFencer{claimed: map[string]int{}}
}

func (f *sharedFencer) Claim(_ context.Context, key string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimed[key]++
	if f.claimed[key] > 1 {
		return ErrFenceMarkerHeld
	}
	return nil
}

func (f *sharedFencer) PriorMetaPrefix(context.Context, string, int64) (string, error) {
	return f.prior, nil
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
}

func newLeaseAuthority(current int64, ttl time.Duration) *leaseAuthority {
	return &leaseAuthority{
		current:  current,
		ttl:      ttl,
		ready:    map[int64]time.Time{},
		fenced:   map[int64]time.Time{},
		released: map[int64][]string{},
	}
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

func (a *leaseAuthority) RenewLease(_ context.Context, _ string, epoch int64) (LeaseResponse, error) {
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

func (a *leaseAuthority) AckGrant(context.Context, string, int64, int64) error { return nil }

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

func (r *countingReplicator) Restore(_ context.Context, src string, _ time.Time) error {
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
func (r *countingReplicator) Stop(context.Context) error { r.record("stop"); return nil }

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

// TestTwoWorkersAtTheSameEpochOnlyOneEverMounts is the same-epoch half of the
// concurrency acceptance, and it is the half the worker itself decides: the
// fence marker is a conditional PUT on one key, so of two workers handed the
// same epoch exactly one claims it and the other exits 66 before it restores
// anything. Neither the control-plane nor a timeout is involved.
func TestTwoWorkersAtTheSameEpochOnlyOneEverMounts(t *testing.T) {
	auth := newLeaseAuthority(9, 2*time.Minute)
	fencer := newSharedFencer()

	type outcome struct {
		fatal *Fatal
		rep   *countingReplicator
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			spec := specAtEpoch(9, 2*time.Minute)
			vol := &readyReportingVolume{fakeVolume: healthyVolume(), auth: auth, epoch: 9}
			rep := &countingReplicator{}
			sup := newCloseoutSup(t, spec, vol, auth, rep, fencer)
			stop := make(chan os.Signal, 1)
			// Both workers are asked to stop shortly after they mount, so the
			// winner reaches a clean stop instead of running to the timeout.
			time.AfterFunc(150*time.Millisecond, func() { stop <- syscall.SIGTERM })
			results <- outcome{fatal: sup.Run(context.Background(), stop), rep: rep}
		}()
	}
	wg.Wait()
	close(results)

	var mounted, fenced int
	for r := range results {
		switch r.fatal.Exit {
		case CodeOK:
			mounted++
		case CodeFenced:
			fenced++
			if r.fatal.ErrCode != ErrCodeFenceMarkerHeld {
				t.Errorf("loser error code = %s, want %s", r.fatal.ErrCode, ErrCodeFenceMarkerHeld)
			}
			// The loser must not have restored or replicated anything: the
			// marker is claimed before the first LTX read or write.
			if n := len(r.rep.events); n != 0 {
				t.Errorf("the fenced-out worker touched the replica %d times, want 0: %v", n, r.rep.events)
			}
		default:
			t.Errorf("unexpected exit %d: %v", r.fatal.Exit, r.fatal.Err)
		}
	}
	if mounted != 1 || fenced != 1 {
		t.Fatalf("mounted=%d fenced=%d, want exactly one of each", mounted, fenced)
	}
	// Both workers share epoch 9 here, so the authority sees two releases: the
	// loser's "fenced" and the winner's "shutdown".
	if !auth.releasedWith(9, "fenced") {
		t.Errorf("no worker released epoch 9 with reason \"fenced\"; saw %q", auth.reasons(9))
	}
	if !auth.releasedWith(9, "shutdown") {
		t.Errorf("the winner did not release epoch 9 cleanly; saw %q", auth.reasons(9))
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
	if oldFatal.ErrCode != ErrCodeLeaseLost {
		t.Errorf("superseded writer error code = %s, want %s", oldFatal.ErrCode, ErrCodeLeaseLost)
	}
	if !oldVol.Fenced() {
		t.Error("the superseded writer exited 66 without fencing its own filesystem")
	}
	if !auth.releasedWith(11, "fenced") {
		t.Errorf("superseded writer released with %q, want \"fenced\"", auth.reasons(11))
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

// TestAFencedWriterStillUploadsOnItsWayOut is a characterisation test for the
// question the design documents leave open, not for a confirmed defect.
//
// On lease loss the supervisor runs fenceAndStop, which runs the full ordered
// shutdown (supervisor.go:612-627 then :638-704): fence, `juicefs durability`
// barrier — which flushes the writeback cache to the SHARED, non-epoch-
// partitioned data prefix — then Replicator.SyncAndWait and Stop, which push
// more LTX into the epoch's metadata prefix. All of those are uploads, and they
// happen after the writer was told it no longer owns the volume.
//
// For the DEADLINE path that is exactly right: threat-model.md §7.2 rejects both
// "fence then flush" and "flush then fence", and requires a bounded flush window
// inside the lease with an incomplete flush reported as data loss. shutdown
// implements that, bounded by the remaining lease (supervisor.go:639-644).
//
// The open question is the OUT-OF-BAND fence — a 412 on the marker, or a
// stale_epoch/lease_held 409 from renew. The threat model states the drain rule
// only in terms of expiry and never says whether staged bytes may still be
// flushed when the epoch was taken away rather than run out. This test forces
// that case by revoking the epoch while the local lease still has ~2 minutes on
// it, and records what the implementation does with it.
//
// Note the setup is deliberately stronger than production can currently
// produce: services/control-plane/internal/storagevol has no force-takeover
// path, so a real stale_epoch only follows a genuine expiry, by which time the
// worker's own margin guard has already fired and its shutdown budget is the
// one-second floor. See finding F-1 in the PR body.
func TestAFencedWriterStillUploadsOnItsWayOut(t *testing.T) {
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

	// Revoke the epoch while the local lease still has ~2 minutes on it.
	auth.promote(21)
	revokedAt := time.Now()

	f := waitFatal(t, done, 3*time.Second, "the revoked writer never exited")
	if f.Exit != CodeFenced {
		t.Fatalf("exit = %d (%v), want %d", f.Exit, f.Err, CodeFenced)
	}

	// The barrier is a data-plane upload and it ran after the fence.
	order := vol.order()
	fenceAt, barrierAt := -1, -1
	for i, c := range order {
		if c == "fence" && fenceAt < 0 {
			fenceAt = i
		}
		if c == "barrier" && fenceAt >= 0 && barrierAt < 0 {
			barrierAt = i
		}
	}
	if barrierAt < 0 {
		t.Log("no barrier runs after the fence any more — finding F-1 is fixed; invert this test")
		return
	}
	if n := rep.uploadsAfter(revokedAt); n == 0 {
		t.Log("no replica upload after revocation any more — finding F-1 is fixed; invert this test")
		return
	}
	t.Logf("documented gap F-1: after losing epoch 20 the worker still ran %v and %d replica upload(s)",
		order[fenceAt:], rep.uploadsAfter(revokedAt))
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
