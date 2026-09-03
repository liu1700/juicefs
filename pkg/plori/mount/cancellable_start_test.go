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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// PLO-393 F-3 and PLO-444. The startup is now cancellable, and the two things
// that were true of the old one -- that a TERM waited for the whole of start()
// and that a failed mount left its FUSE session running -- are what these tests
// pin against.
//
// They reuse the PLO-438 timeline doubles (start_teardown_test.go): the whole
// question is what happened in which order, and separate call lists per double
// cannot answer it.

const evServeReturned = "volume.serve_returned"

// ------------------------------------------------------------- the doubles ---

// signallingReplicator fires the stop signal from INSIDE a startup step and
// stays in that step until the startup context is cancelled, which is how "a
// TERM arrived during the restore" is tested without a sleep that races the
// supervisor.
type signallingReplicator struct {
	*fakeReplicator
	tl *timeline
	// during names the step that fires the signal: "restore" or "sync".
	during string
	stop   chan os.Signal

	restored atomic.Bool
	seeded   atomic.Bool
}

func (r *signallingReplicator) fire(ctx context.Context, step string) {
	if r.during != step {
		return
	}
	r.stop <- syscall.SIGTERM
	// Wait for the cancellation, never for a duration. This step stands in for
	// the rest of a restore that is already under way, and the event that ends
	// it is Run's watcher cancelling the startup context; a sleep here is only
	// a guess at how long the scheduler takes to get there, and on a starved
	// one the guess is wrong (PLO-479). serveVolume.AwaitMounted waits the
	// same way, on the same event.
	<-ctx.Done()
}

func (r *signallingReplicator) Restore(ctx context.Context, src string, opt RestoreOptions) error {
	r.fire(ctx, "restore")
	r.restored.Store(true)
	return r.fakeReplicator.Restore(ctx, src, opt)
}

func (r *signallingReplicator) Start(ctx context.Context) error {
	r.tl.add(evReplicatorStart)
	return r.fakeReplicator.Start(ctx)
}

func (r *signallingReplicator) SyncAndWait(ctx context.Context) error {
	r.tl.add(evReplicatorSync)
	r.fire(ctx, "sync")
	r.seeded.Store(true)
	return r.fakeReplicator.SyncAndWait(ctx)
}

func (r *signallingReplicator) Stop(ctx context.Context) error {
	r.tl.add(evReplicatorStop)
	return r.fakeReplicator.Stop(ctx)
}

func (r *signallingReplicator) Abort(ctx context.Context) error {
	r.tl.add(evReplicatorAbort)
	return r.fakeReplicator.Abort(ctx)
}

// serveVolume is a volume whose FUSE session ends only when its context is
// cancelled, and whose mount never comes up. It is the shape PLO-444 is about:
// AwaitMounted fails while Serve is still in flight.
type serveVolume struct {
	*fakeVolume
	tl         *timeline
	awaitErr   error
	serveErr   error
	serveDelay time.Duration
	// awaitStop makes the wait for the mount fire the stop signal and then wait
	// for its own context, which is what a real FUSE wait interrupted by a TERM
	// does: it fails with the cancellation, in its own vocabulary.
	awaitStop chan os.Signal
	closes    atomic.Int32
}

func (v *serveVolume) Serve(ctx context.Context) error {
	<-ctx.Done()
	time.Sleep(v.serveDelay)
	v.tl.add(evServeReturned)
	return v.serveErr
}

func (v *serveVolume) AwaitMounted(ctx context.Context) error {
	if v.awaitStop != nil {
		v.awaitStop <- syscall.SIGTERM
		<-ctx.Done()
		return ctx.Err()
	}
	return v.awaitErr
}

func (v *serveVolume) Unmount(ctx context.Context) error {
	v.tl.add(evVolumeUnmount)
	return v.fakeVolume.Unmount(ctx)
}

func (v *serveVolume) Close() error {
	v.closes.Add(1)
	v.tl.add(evVolumeClose)
	return v.fakeVolume.Close()
}

// supAt builds a supervisor over a state directory the caller chooses, so two
// generations can be run over the same node-local state.
func supAt(t *testing.T, dir string, spec *MountSpec, vol Volume, cp ControlPlane, rep Replicator, fencer Fencer) *Supervisor {
	t.Helper()
	return &Supervisor{
		Spec: spec,
		Paths: Paths{
			StateDir:   filepath.Join(dir, "state"),
			CacheDir:   filepath.Join(dir, "cache"),
			MountPoint: filepath.Join(dir, "mnt"),
		},
		Options: MountOptions{BarrierInterval: 30 * time.Millisecond},
		Deps: Deps{
			FS:                   &tracedFS{vol: vol},
			CP:                   cp,
			Replicator:           rep,
			Fencer:               fencer,
			ControlGateInstalled: func() bool { return true },
		},
	}
}

// --------------------------------------------------------------- the tests ---

// The headline case: the signal lands while the restore is running. Before
// PLO-393 the restore ran to completion, the mount came up, and only then did
// the ordered stop begin -- which on a large replica is where the kubelet's
// grace period runs out and SIGKILL lands instead.
func TestATermDuringTheRestoreAbortsTheStartupAndExitsZero(t *testing.T) {
	tl := &timeline{}
	stop := make(chan os.Signal, 2)
	rep := &signallingReplicator{
		fakeReplicator: &fakeReplicator{}, tl: tl,
		during: "restore", stop: stop,
	}
	cp := &tracedCP{fakeCP: &fakeCP{}, tl: tl}
	vol := &tracedVolume{fakeVolume: healthyVolume(), tl: tl}
	sup := supAt(t, t.TempDir(), testSpec(), vol, cp, rep, &fakeFencer{})

	got := sup.Run(context.Background(), stop)

	if got.Exit != CodeOK || got.ErrCode != ErrCodeStoppedBeforeMount {
		t.Fatalf("exit = %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeOK, ErrCodeStoppedBeforeMount, got.Err)
	}
	if !rep.restored.Load() {
		t.Fatal("the restore never ran; this test no longer exercises a TERM DURING the restore")
	}
	// The step is named on the one stderr line the plugin republishes, because
	// "cancelled before touching the store" and "cancelled after a half-hour
	// restore" are different operational facts.
	if !strings.Contains(got.Err.Error(), "the metadata restore") {
		t.Errorf("the exit message does not say how far the startup got: %q", got.Err.Error())
	}
	// Nothing past the restore ran: the abort is an abort, not a deferral.
	if tl.at(evReplicatorStart) >= 0 {
		t.Errorf("replication was started after the stop signal; timeline was %v", tl.events())
	}
	if readyExists(t, sup) {
		t.Error("an abandoned startup must never publish a ready file")
	}
	if cp.released != ReasonStoppedBeforeMount {
		t.Errorf("release reason = %q, want %q", cp.released, ReasonStoppedBeforeMount)
	}
}

// The same signal one step later, after the replicator is up. This is the case
// that has something to tear down, so it must run PLO-438's ordered teardown --
// replicator aborted, volume closed, and only then the lease handed back.
func TestATermDuringTheSeedAbortsThroughTheOrderedTeardown(t *testing.T) {
	tl := &timeline{}
	stop := make(chan os.Signal, 2)
	rep := &signallingReplicator{
		fakeReplicator: &fakeReplicator{restoreErr: ErrReplicaEmpty}, tl: tl,
		during: "sync", stop: stop,
	}
	cp := &tracedCP{fakeCP: &fakeCP{}, tl: tl}
	vol := &tracedVolume{fakeVolume: healthyVolume(), tl: tl}
	sup := supAt(t, t.TempDir(), bootstrapSpec(), vol, cp, rep, &fakeFencer{})

	got := sup.Run(context.Background(), stop)

	if got.Exit != CodeOK || got.ErrCode != ErrCodeStoppedBeforeMount {
		t.Fatalf("exit = %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeOK, ErrCodeStoppedBeforeMount, got.Err)
	}
	if !rep.seeded.Load() {
		t.Fatal("the seed never ran; this test no longer exercises a TERM DURING the seed")
	}
	if !strings.Contains(got.Err.Error(), "the replica seed") {
		t.Errorf("the exit message does not say how far the startup got: %q", got.Err.Error())
	}
	// The whole of PLO-438's invariant, on the abort path: aborted, closed,
	// released, in that order and with nothing left running behind the release.
	tl.requireOrder(t, evReplicatorStart, evReplicatorAbort, evVolumeClose, evLeaseRelease)
	tl.requireOrderlyTeardown(t)
	if tl.at(evReplicatorStop) >= 0 {
		t.Errorf("an abandoned startup must ABORT the replicator, not stop it; timeline was %v", tl.events())
	}
	if readyExists(t, sup) {
		t.Error("an abandoned startup must never publish a ready file")
	}
	if cp.released != ReasonStoppedBeforeMount {
		t.Errorf("release reason = %q, want %q", cp.released, ReasonStoppedBeforeMount)
	}
}

// The bound on the whole feature: a cancelled restore must not leave behind a
// metadata database the NEXT generation adopts. Adoption (PLO-422) is gated on
// the `clean` marker, and start() removes that marker before it restores, so an
// abort in the middle leaves a database with no proof of what it contains --
// which is exactly the state reconcileLocalDatabase sets aside.
//
// Driven as two generations over one state directory rather than asserted
// against the rule, because the rule is only half the story: the other half is
// that nothing on the abort path writes a `clean` marker.
func TestACancelledRestoreLeavesNoDatabaseTheNextGenerationWouldAdopt(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(stateDir, "meta.db")

	// Generation N died without finishing its stop, so its database is set
	// aside and generation N+1 restores. The durable point it left is kept
	// deliberately: a point without the `clean` marker is not evidence, and the
	// restore has to happen anyway (localdb.go adoptable).
	spec := testSpec()
	if err := os.WriteFile(metaPath, []byte("generation N"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(filepath.Join(stateDir, "durable-point.json"), DurablePoint{
		Volume: spec.StorageVolumeID, FenceEpoch: spec.FenceEpoch - 1,
		DurableAt: time.Now().UTC().Add(-time.Minute), BarrierAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	// Generation N+1 is interrupted while it restores. Its restore is a real
	// half-write: the database on disk is neither the old one nor a whole new
	// one, which is the state that must never be adopted.
	tl := &timeline{}
	stop := make(chan os.Signal, 2)
	rep := &signallingReplicator{
		fakeReplicator: &fakeReplicator{}, tl: tl,
		during: "restore", stop: stop,
	}
	rep.fakeReplicator.restoreErr = nil
	cut := &cuttingReplicator{signallingReplicator: rep, metaPath: metaPath}
	cp := &tracedCP{fakeCP: &fakeCP{}, tl: tl}
	supN1 := supAt(t, dir, spec, &tracedVolume{fakeVolume: healthyVolume(), tl: tl}, cp, cut, &fakeFencer{})

	if got := supN1.Run(context.Background(), stop); got.Exit != CodeOK || got.ErrCode != ErrCodeStoppedBeforeMount {
		t.Fatalf("exit = %d / %s, want the abort (%v)", got.Exit, got.ErrCode, got.Err)
	}
	if exists(t, supN1.Paths.CleanStopPath()) {
		t.Fatal("the abort wrote a clean marker; the next generation would adopt a half-restored database")
	}
	// The rule the marker's absence feeds, stated directly: whatever else is on
	// disk, a generation that did not finish its stop is never adopted.
	local, _ := ReadDurablePoint(supN1.Paths.DurablePointPath())
	if reason, ok := adoptable(spec.StorageVolumeID, false, local, 0); ok {
		t.Fatalf("a half-restored database was judged adoptable: %s", reason)
	}

	// Generation N+2, on the same node. It must restore rather than adopt, and
	// the half-written file must be out of the way rather than under the new
	// one.
	next := testSpec()
	next.FenceEpoch = spec.FenceEpoch + 1
	repN2 := &fakeReplicator{}
	supN2 := supAt(t, dir, next, &tracedVolume{fakeVolume: healthyVolume(), tl: &timeline{}},
		&fakeCP{}, repN2, &fakeFencer{})
	if got := supN2.Run(context.Background(), stopOnReady(supN2)); got.Exit != CodeOK {
		t.Fatalf("the successor exited %d / %s (%v)", got.Exit, got.ErrCode, got.Err)
	}
	var restored bool
	for _, c := range repN2.order() {
		if c == "restore" {
			restored = true
		}
	}
	if !restored {
		t.Error("the successor ADOPTED the half-restored database instead of restoring the replica")
	}
	if !exists(t, metaPath+supersededSuffix) {
		t.Error("the half-restored database was not set aside")
	}
}

// PLO-444. AwaitMounted fails while Serve is still in flight; the session's own
// error is the only thing that knows why, and the teardown must not run on top
// of it.
func TestAServeThatFailsAfterTheMountDeadlineIsJoinedAndReported(t *testing.T) {
	tl := &timeline{}
	vol := &serveVolume{
		fakeVolume: healthyVolume(), tl: tl,
		awaitErr: errors.New("root inode never answered"),
		serveErr: errors.New("fuse: mount point is not empty"),
		// The session takes a moment to unwind after its context is cancelled.
		// Without the join, Close would land inside this window.
		serveDelay: 40 * time.Millisecond,
	}
	cp := &tracedCP{fakeCP: &fakeCP{}, tl: tl}
	rep := &tracedReplicator{fakeReplicator: &fakeReplicator{}, tl: tl}
	sup := supAt(t, t.TempDir(), testSpec(), vol, cp, rep, &fakeFencer{})

	got := sup.Run(context.Background(), make(chan os.Signal))

	// Unchanged: a mount that never came up is the 67 class and is retryable.
	if got.Exit != CodeRestoreFailed || got.ErrCode != ErrCodeRestoreFailed {
		t.Fatalf("exit = %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeRestoreFailed, ErrCodeRestoreFailed, got.Err)
	}
	msg := got.Err.Error()
	if !strings.Contains(msg, "mount did not become ready") {
		t.Errorf("the exit message lost the deadline that failed: %q", msg)
	}
	if !strings.Contains(msg, "mount point is not empty") {
		t.Errorf("the exit message does not carry what the fuse session returned, which is the real cause: %q", msg)
	}
	if n := vol.closes.Load(); n != 1 {
		t.Errorf("Close ran %d times, want exactly once", n)
	}
	// The ordering the double-close race is about.
	tl.requireOrder(t, evServeReturned, evVolumeClose, evLeaseRelease)
	tl.requireOrderlyTeardown(t)
	if cp.released != "mount_failed" {
		t.Errorf("release reason = %q, want %q", cp.released, "mount_failed")
	}
}

// The last pre-ready step, and the one that is NOT a checkpoint: the wait for
// the FUSE mount. It runs under the startup context, so a TERM during it makes
// AwaitMounted fail in its own vocabulary — and that failure is the stop, not a
// mount refusal the plugin should retry. This is preReady's rule reached
// through the real path, and it exercises PLO-444's join at the same time.
func TestATermWhileWaitingForTheMountIsTheStopNotAMountFailure(t *testing.T) {
	tl := &timeline{}
	stop := make(chan os.Signal, 2)
	vol := &serveVolume{
		fakeVolume: healthyVolume(), tl: tl,
		awaitStop: stop, serveDelay: 20 * time.Millisecond,
	}
	cp := &tracedCP{fakeCP: &fakeCP{}, tl: tl}
	rep := &tracedReplicator{fakeReplicator: &fakeReplicator{}, tl: tl}
	sup := supAt(t, t.TempDir(), testSpec(), vol, cp, rep, &fakeFencer{})

	got := sup.Run(context.Background(), stop)

	if got.Exit != CodeOK || got.ErrCode != ErrCodeStoppedBeforeMount {
		t.Fatalf("exit = %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeOK, ErrCodeStoppedBeforeMount, got.Err)
	}
	if got.Retryable {
		t.Error("a cancelled startup is not something the plugin should retry")
	}
	if readyExists(t, sup) {
		t.Error("a startup cancelled at the mount must never publish a ready file")
	}
	// The session is still joined before the teardown, and the lease reports the
	// stop rather than a mount failure the operator would chase.
	tl.requireOrder(t, evServeReturned, evVolumeClose, evLeaseRelease)
	if cp.released != ReasonStoppedBeforeMount {
		t.Errorf("release reason = %q, want %q", cp.released, ReasonStoppedBeforeMount)
	}
}

// preReady is the one rule that turns any pre-ready failure into the abort once
// the startup context is done. It is worth a direct test because the steps fail
// in their own vocabulary and the rule has to survive all of them.
func TestPreReadyReportsTheStopRatherThanTheStepThatNoticedIt(t *testing.T) {
	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	// Not stopping: every failure keeps its own code.
	store := fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true, "claim fence marker: connection refused")
	if got := preReady(live, store); got.Exit != CodeObjectStore || got.ErrCode != ErrCodeObjectStoreUnreachable {
		t.Errorf("a live startup reclassified a store failure as %d / %s", got.Exit, got.ErrCode)
	}
	// Stopping: the same failure IS the stop.
	got := preReady(cancelled, store)
	if got.Exit != CodeOK || got.ErrCode != ErrCodeStoppedBeforeMount {
		t.Errorf("got %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeOK, ErrCodeStoppedBeforeMount)
	}
	if !strings.Contains(got.Err.Error(), "connection refused") {
		t.Errorf("the cause was dropped: %q", got.Err.Error())
	}
	// An abort a checkpoint already produced is not wrapped twice.
	abort := stoppedBeforeMount("the metadata restore")
	if got := preReady(cancelled, abort); got != abort {
		t.Errorf("a checkpoint's own abort was rewrapped: %q", got.Err.Error())
	}
}

// cuttingReplicator restores by half-writing the database, which is what makes
// the file on disk unusable evidence rather than merely stale.
type cuttingReplicator struct {
	*signallingReplicator
	metaPath string
}

func (r *cuttingReplicator) Restore(ctx context.Context, src string, opt RestoreOptions) error {
	if err := os.WriteFile(r.metaPath, []byte("half a restore"), 0o600); err != nil {
		return err
	}
	return r.signallingReplicator.Restore(ctx, src, opt)
}
