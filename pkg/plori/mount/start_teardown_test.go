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
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
)

// PLO-438. A startup that fails after the replicator is up has to put the
// replicator and the volume down BEFORE it hands the lease back, and the
// existing doubles cannot show that: each records its own calls in its own
// list, so "the replicator stopped" and "the lease was released" are two
// unordered facts. These tests give the three doubles ONE timeline, which is
// the only thing that can answer the question the issue asks.

const (
	evReplicatorStart = "replicator.start"
	evReplicatorSync  = "replicator.sync"
	evReplicatorStop  = "replicator.stop"
	evReplicatorAbort = "replicator.abort"
	evVolumeUnmount   = "volume.unmount"
	evVolumeClose     = "volume.close"
	evLeaseRelease    = "lease.release"
)

type timeline struct {
	mu sync.Mutex
	ev []string
}

func (t *timeline) add(e string) {
	t.mu.Lock()
	t.ev = append(t.ev, e)
	t.mu.Unlock()
}

func (t *timeline) events() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.ev...)
}

// at is where `e` first happened, or -1 if it never did.
func (t *timeline) at(e string) int {
	for i, got := range t.events() {
		if got == e {
			return i
		}
	}
	return -1
}

// detachedAt is where the replicator stopped receiving this database, by
// whichever of the two calls did it. Both are "detach" as far as the fault in
// PLO-438 is concerned: on the per-mount path they are a graceful stop and a
// kill of the child, and on the node daemon they are two ways of spelling
// `POST /unregister`. Asserting on the property rather than on the call is
// what lets one assertion cover the start path and the ordered stop.
func (t *timeline) detachedAt() int {
	stop, abort := t.at(evReplicatorStop), t.at(evReplicatorAbort)
	switch {
	case stop < 0:
		return abort
	case abort < 0:
		return stop
	case abort < stop:
		return abort
	default:
		return stop
	}
}

// requireOrder fails unless every named event happened, in the order named.
func (t *timeline) requireOrder(tb testing.TB, want ...string) {
	tb.Helper()
	prev := -1
	for _, e := range want {
		at := t.at(e)
		if at < 0 {
			tb.Fatalf("%s never happened; timeline was %v", e, t.events())
		}
		if at <= prev {
			tb.Fatalf("%s happened out of order; timeline was %v", e, t.events())
		}
		prev = at
	}
}

// requireOrderlyTeardown is the invariant the whole issue is about: nothing
// that can still write to this epoch's prefix may outlive the lease release,
// because the release is what lets a successor claim the volume.
func (t *timeline) requireOrderlyTeardown(tb testing.TB) {
	tb.Helper()
	released := t.at(evLeaseRelease)
	if released < 0 {
		tb.Fatalf("the lease was never released; timeline was %v", t.events())
	}
	if d := t.detachedAt(); d < 0 || d > released {
		tb.Fatalf("the replicator was still attached when the lease was released; timeline was %v", t.events())
	}
	if c := t.at(evVolumeClose); c < 0 || c > released {
		tb.Fatalf("the volume was still open when the lease was released; timeline was %v", t.events())
	}
}

// ------------------------------------------------------------- the doubles ---

type tracedReplicator struct {
	*fakeReplicator
	tl *timeline
	// abortErr is a teardown that itself fails. The worker must report it and
	// carry on to the lease release, never hang on it.
	abortErr error
}

func (r *tracedReplicator) Start(ctx context.Context) error {
	r.tl.add(evReplicatorStart)
	return r.fakeReplicator.Start(ctx)
}

func (r *tracedReplicator) SyncAndWait(ctx context.Context) error {
	r.tl.add(evReplicatorSync)
	return r.fakeReplicator.SyncAndWait(ctx)
}

func (r *tracedReplicator) Stop(ctx context.Context) error {
	r.tl.add(evReplicatorStop)
	return r.fakeReplicator.Stop(ctx)
}

func (r *tracedReplicator) Abort(ctx context.Context) error {
	r.tl.add(evReplicatorAbort)
	if err := r.fakeReplicator.Abort(ctx); err != nil {
		return err
	}
	return r.abortErr
}

type tracedVolume struct {
	*fakeVolume
	tl *timeline
	// awaitErr is a FUSE mount that never comes up.
	awaitErr error
}

func (v *tracedVolume) AwaitMounted(ctx context.Context) error {
	if v.awaitErr != nil {
		return v.awaitErr
	}
	return v.fakeVolume.AwaitMounted(ctx)
}

func (v *tracedVolume) Unmount(ctx context.Context) error {
	v.tl.add(evVolumeUnmount)
	return v.fakeVolume.Unmount(ctx)
}

func (v *tracedVolume) Close() error {
	v.tl.add(evVolumeClose)
	return v.fakeVolume.Close()
}

type tracedCP struct {
	*fakeCP
	tl *timeline
}

func (c *tracedCP) ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error {
	c.tl.add(evLeaseRelease)
	return c.fakeCP.ReleaseLease(ctx, volumeID, epoch, reason)
}

type tracedFS struct{ vol Volume }

func (f *tracedFS) Format(context.Context, *MountSpec) error { return nil }
func (f *tracedFS) Open(context.Context, *MountSpec) (Volume, error) {
	return f.vol, nil
}

// tracedSup is newSup with the three doubles rewired onto one timeline.
func tracedSup(t *testing.T, spec *MountSpec, rep *fakeReplicator, cp *fakeCP) (*Supervisor, *timeline, *tracedReplicator, *tracedVolume) {
	t.Helper()
	tl := &timeline{}
	vol := &tracedVolume{fakeVolume: healthyVolume(), tl: tl}
	tr := &tracedReplicator{fakeReplicator: rep, tl: tl}
	tc := &tracedCP{fakeCP: cp, tl: tl}
	sup := newSup(t, spec, &fakeFS{vol: vol.fakeVolume}, cp, rep, &fakeFencer{})
	sup.Deps.FS = &tracedFS{vol: vol}
	sup.Deps.Replicator = tr
	sup.Deps.CP = tc
	return sup, tl, tr, vol
}

// --------------------------------------------------------------- the tests ---

// The fault as reported: a brand-new volume is formatted, the replicator comes
// up, and the seed sync that pushes the first replica fails. Before PLO-438 the
// worker released the lease and exited with the replicator still running — a
// registration on the node daemon that keeps syncing this epoch's prefix while
// a successor restores (PLO-323 F-1), or an orphan Litestream child holding
// meta.db open, which PLO-421's Setpgid put out of reach of the plugin's group
// kill.
func TestAFailedSeedSyncStopsTheReplicatorBeforeItReleasesTheLease(t *testing.T) {
	cp := &fakeCP{}
	sup, tl, _, _ := tracedSup(t, bootstrapSpec(),
		&fakeReplicator{restoreErr: ErrReplicaEmpty, syncErr: errors.New("PUT: connection refused")}, cp)

	got := sup.Run(context.Background(), make(chan os.Signal))

	// The exit code is the pre-existing one for the step that failed: the seed
	// is an object-store write, and a store that refused it is retryable.
	if got.Exit != CodeObjectStore || got.ErrCode != ErrCodeObjectStoreUnreachable {
		t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeObjectStore, ErrCodeObjectStoreUnreachable, got.Err)
	}
	if !got.Retryable {
		t.Error("a store that refused the seed is retryable; the plugin republishes this")
	}
	// The whole point: aborted, closed, and only then handed back.
	tl.requireOrder(t, evReplicatorStart, evReplicatorSync, evReplicatorAbort, evVolumeClose, evLeaseRelease)
	tl.requireOrderlyTeardown(t)
	// Aborted, not stopped. A graceful stop syncs, and the only thing this
	// generation has to sync is a format it is abandoning — history in a
	// prefix its successor restores from, anchored by nothing.
	if tl.at(evReplicatorStop) >= 0 {
		t.Errorf("the start path must ABORT the replicator, not stop it; timeline was %v", tl.events())
	}
	if cp.released != "object_store_unreachable" {
		t.Errorf("release reason = %q, want %q", cp.released, "object_store_unreachable")
	}
	if readyExists(t, sup) {
		t.Error("a mount that never seeded its replica must never be published to the Agent")
	}
}

// The same invariant on the ack path (fork #50 / PLO-420). It already held —
// that refusal goes through the ordered stop — and this is the guard that says
// so, because the two paths are one line apart in Run and the issue is exactly
// that one of them forgot.
func TestARefusedFormatAckStopsTheReplicatorBeforeItReleasesTheLease(t *testing.T) {
	cp := &fakeCP{ackErr: &CPError{
		Status: http.StatusConflict, Code: CPCodeFormatMismatch, Msg: "volume already carries a different format"}}
	sup, tl, _, _ := tracedSup(t, bootstrapSpec(), &fakeReplicator{restoreErr: ErrReplicaEmpty}, cp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := sup.Run(ctx, make(chan os.Signal))

	// Unchanged: the control-plane does not know what filesystem this is, which
	// is the invariant exit 65 names.
	if got.Exit != CodeIdentityMismatch || got.ErrCode != ErrCodeIdentityMismatch {
		t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeIdentityMismatch, ErrCodeIdentityMismatch, got.Err)
	}
	tl.requireOrderlyTeardown(t)
	if cp.released != "identity_mismatch" {
		t.Errorf("release reason = %q, want %q", cp.released, "identity_mismatch")
	}
}

// And on the mount path. AwaitMounted failing is the third way a start dies
// with a replicator running, and it too must not leave one behind.
func TestAMountThatNeverComesUpStopsTheReplicatorBeforeItReleasesTheLease(t *testing.T) {
	cp := &fakeCP{}
	sup, tl, _, vol := tracedSup(t, testSpec(), &fakeReplicator{}, cp)
	vol.awaitErr = errors.New("root inode never answered")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := sup.Run(ctx, make(chan os.Signal))

	if got.Exit != CodeRestoreFailed || got.ErrCode != ErrCodeRestoreFailed {
		t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeRestoreFailed, ErrCodeRestoreFailed, got.Err)
	}
	tl.requireOrderlyTeardown(t)
	if cp.released != "mount_failed" {
		t.Errorf("release reason = %q, want %q", cp.released, "mount_failed")
	}
}

// A teardown that fails is a fact about the node — something of this worker's
// is still running on it — so it is reported on the one stderr line the plugin
// republishes. What it may NOT do is change the exit code (the plugin's
// handling of the mount is decided by the step that failed, not by how tidy
// the cleanup was) or skip the lease release (the Agent would wait a whole TTL
// for a mount that never was).
func TestAFailedStartTeardownIsReportedAndStillReleasesTheLease(t *testing.T) {
	cp := &fakeCP{}
	sup, tl, rep, _ := tracedSup(t, bootstrapSpec(),
		&fakeReplicator{restoreErr: ErrReplicaEmpty, syncErr: errors.New("PUT: connection refused")}, cp)
	rep.abortErr = errors.New("control socket: no such file or directory")

	got := sup.Run(context.Background(), make(chan os.Signal))

	if got.Exit != CodeObjectStore || got.ErrCode != ErrCodeObjectStoreUnreachable {
		t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeObjectStore, ErrCodeObjectStoreUnreachable, got.Err)
	}
	msg := got.Err.Error()
	if !strings.Contains(msg, "seed replica") {
		t.Errorf("the exit message lost the step that failed: %q", msg)
	}
	if !strings.Contains(msg, "start teardown incomplete") || !strings.Contains(msg, "control socket") {
		t.Errorf("the exit message does not say what was left running: %q", msg)
	}
	tl.requireOrder(t, evReplicatorAbort, evVolumeClose, evLeaseRelease)
	if cp.released != "object_store_unreachable" {
		t.Errorf("release reason = %q, want %q", cp.released, "object_store_unreachable")
	}
}

// A start that fails BEFORE the replicator is up has nothing to tear down, and
// must not pretend otherwise: an Abort against a replicator that was never
// started is a stray unregister on the node daemon, which on a shared socket is
// a call about somebody else's database.
func TestAStartThatFailsBeforeReplicationNeverTouchesTheReplicator(t *testing.T) {
	cp := &fakeCP{}
	sup, tl, _, _ := tracedSup(t, testSpec(), &fakeReplicator{}, cp)
	sup.Deps.Fencer = &fakeFencer{err: ErrFenceMarkerHeld}

	got := sup.Run(context.Background(), make(chan os.Signal))

	if got.Exit != CodeFenced || got.ErrCode != ErrCodeFenceMarkerHeld {
		t.Fatalf("got exit %d / %s, want %d / %s (%v)", got.Exit, got.ErrCode, CodeFenced, ErrCodeFenceMarkerHeld, got.Err)
	}
	if tl.detachedAt() >= 0 {
		t.Errorf("a replicator that was never started must not be torn down; timeline was %v", tl.events())
	}
	if tl.at(evLeaseRelease) < 0 {
		t.Error("the lease still goes back")
	}
}
