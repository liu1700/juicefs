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
	"syscall"
	"testing"
	"time"
)

// PLO-326 acceptance close-out: the state-machine artefacts, the idempotency
// bullet, and the exit-signal taxonomy.
//
// The ordered stop itself is already covered by TestSigtermRunsTheOrderedShutdown
// in supervisor_test.go. What was missing is everything the NEXT generation and
// the plugin actually read: which files the stop leaves behind, whether a
// repeated stop is safe, and whether the six terminal conditions the issue
// lists are distinguishable from outside the process.

// countingVolume adds per-method call counts on top of fakeVolume so a repeated
// stop can be told from a single one.
type countingVolume struct {
	*fakeVolume
	unmounts int
	closes   int
	barriers int
}

func (v *countingVolume) Unmount(ctx context.Context) error {
	v.mu.Lock()
	v.unmounts++
	v.mu.Unlock()
	return v.fakeVolume.Unmount(ctx)
}

func (v *countingVolume) Close() error {
	v.mu.Lock()
	v.closes++
	v.mu.Unlock()
	return v.fakeVolume.Close()
}

func (v *countingVolume) Barrier(ctx context.Context) (BarrierResult, error) {
	v.mu.Lock()
	v.barriers++
	v.mu.Unlock()
	return v.fakeVolume.Barrier(ctx)
}

func (v *countingVolume) counts() (unmounts, closes, barriers int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.unmounts, v.closes, v.barriers
}

func newCountingVolume() *countingVolume {
	return &countingVolume{fakeVolume: healthyVolume()}
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat %s: %s", path, err)
	}
	return err == nil
}

// stopAfter delivers one SIGTERM once the worker has had time to reach its run
// loop.
func stopAfter(d time.Duration) chan os.Signal {
	ch := make(chan os.Signal, 2)
	time.AfterFunc(d, func() { ch <- syscall.SIGTERM })
	return ch
}

// TestACleanStopLeavesTheThreeFilesTheContractPromises pins the state-dir
// contract from the CLI contract's "State-dir files" section. `ready` tells the
// plugin the mount is usable, `clean` tells the NEXT generation that this one
// finished its ordered stop (its absence is what triggers the unconditional
// repair), and `durable-point.json` is the restore anchor that generation uses.
func TestACleanStopLeavesTheThreeFilesTheContractPromises(t *testing.T) {
	vol := newCountingVolume()
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	for _, p := range []string{sup.Paths.ReadyPath(), sup.Paths.CleanStopPath(), sup.Paths.DurablePointPath()} {
		if !exists(t, p) {
			t.Errorf("a clean stop did not leave %s", filepath.Base(p))
		}
	}
	dp, err := ReadDurablePoint(sup.Paths.DurablePointPath())
	if err != nil || dp == nil {
		t.Fatalf("read durable point: %v %v", dp, err)
	}
	if dp.FenceEpoch != sup.Spec.FenceEpoch {
		t.Errorf("durable point epoch = %d, want %d", dp.FenceEpoch, sup.Spec.FenceEpoch)
	}
	if dp.DurableAt.IsZero() || !dp.DurableAt.Before(dp.BarrierAt.Add(time.Millisecond)) {
		t.Errorf("durable point %+v: DurableAt must be the pre-barrier instant", dp)
	}
	if cp.released != "shutdown" {
		t.Errorf("released with reason %q, want \"shutdown\"", cp.released)
	}
}

// TestAnUncleanStopLeavesNoCleanMarker is the other half of the same signal.
// A stop whose barrier did not complete must exit 69 and must NOT write the
// clean marker, because the next generation reads that marker's absence as
// "run the repair" (supervisor.go:226, :350-364).
func TestAnUncleanStopLeavesNoCleanMarker(t *testing.T) {
	vol := newCountingVolume()
	vol.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{}, errors.New("writeback cache still owes 12 blocks")
	}
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeBarrierIncomplete {
		t.Fatalf("exit = %d (%v), want %d", f.Exit, f.Err, CodeBarrierIncomplete)
	}
	if exists(t, sup.Paths.CleanStopPath()) {
		t.Error("an incomplete stop wrote the clean marker; the next generation would skip its repair")
	}
	// The lease is still handed back: holding it costs the Agent a full TTL and
	// the data is already lost either way.
	if cp.released == "" {
		t.Error("an incomplete stop did not release the lease")
	}
	// And nothing deleted the anchor the previous generation may have left.
	if exists(t, sup.Paths.DurablePointPath()) {
		t.Log("note: a durable point survives an incomplete stop, which is correct — " +
			"no cleanup path may delete the last restorable anchor")
	}
}

// TestASecondSigtermDoesNotRunTheStopTwice is PLO-326's "make repeated
// TERM/unmount/NodeUnpublish idempotent".
//
// Repeated NodeUnpublish is idempotent at the process boundary by construction:
// the run loop returns as soon as it has run the stop once, so a second signal
// is left in the buffered channel and never read, and a second NodeUnpublish
// after the process has exited signals nothing at all. This test proves the
// in-process half — one stop, one unmount, one close — because nothing in the
// supervisor guards shutdown() itself against a second entry.
func TestASecondSigtermDoesNotRunTheStopTwice(t *testing.T) {
	vol := newCountingVolume()
	sup := newCloseoutSup(t, testSpec(), vol, &fakeCP{}, &fakeReplicator{}, newSharedFencer())

	stop := make(chan os.Signal, 2)
	time.AfterFunc(120*time.Millisecond, func() {
		stop <- syscall.SIGTERM
		stop <- syscall.SIGTERM
	})

	f := sup.Run(context.Background(), stop)
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	unmounts, closes, _ := vol.counts()
	if unmounts != 1 {
		t.Errorf("Unmount called %d times, want exactly 1", unmounts)
	}
	if closes != 1 {
		t.Errorf("Close called %d times, want exactly 1", closes)
	}
}

// TestATermArrivingDuringStartupIsDeferredUntilTheMountIsUp is a
// characterisation test for a gap this audit found against PLO-326's "TERM
// before mount" and "TERM during restore" corner cases.
//
// Run does the whole of start() — fence-marker claim, restore, open, integrity
// check, identity match, session purge, repair, replication start — before it
// ever selects on the stop channel (supervisor.go:89-116 then :427-447). The
// signal is buffered by cmd (chan cap 2, cmd/plori_mount.go:127) and is acted
// on only once the mount is fully up, so a TERM during a slow restore does not
// abort it: the worker finishes coming up and then immediately tears back down.
//
// It is safe — nothing is left half-mounted, and the lease is released — but it
// is not the "abort early" the corner case implies, and on a large replica the
// kubelet's grace period can expire during the restore, turning a clean stop
// into a SIGKILL. See finding F-3.
func TestATermArrivingDuringStartupIsDeferredUntilTheMountIsUp(t *testing.T) {
	vol := newCountingVolume()
	rep := &slowReplicator{delay: 150 * time.Millisecond}
	sup := newCloseoutSup(t, testSpec(), vol, &fakeCP{}, rep, newSharedFencer())

	// The signal is already waiting before Run is called: the most extreme
	// form of "TERM before mount".
	stop := make(chan os.Signal, 2)
	stop <- syscall.SIGTERM

	f := sup.Run(context.Background(), stop)
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	if !rep.restored() {
		t.Error("restore was skipped; this test no longer exercises the deferral")
	}
	// The documented gap: the mount came all the way up first.
	if !exists(t, sup.Paths.ReadyPath()) {
		t.Log("a TERM before mount now aborts startup — finding F-3 is fixed; invert this test")
		return
	}
	t.Log("documented gap F-3: a TERM delivered before the mount was up still ran the whole " +
		"startup chain (restore included) before the ordered stop began")
}

type slowReplicator struct {
	fakeReplicator
	delay time.Duration
	did   bool
}

func (r *slowReplicator) Restore(ctx context.Context, src string, opt RestoreOptions) error {
	time.Sleep(r.delay)
	r.mu.Lock()
	r.did = true
	r.mu.Unlock()
	return r.fakeReplicator.Restore(ctx, src, opt)
}

func (r *slowReplicator) restored() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.did
}

// TestExitSignalsDistinguishTheTerminalConditions walks PLO-326's list —
// "distinguish clean stop, lease loss, replication failure, durability timeout,
// local-disk loss and corruption in exit/status signals" — and records what the
// plugin can actually tell apart.
//
// Five of the six are distinguishable. Replication failure and durability
// timeout are not: both are folded into exit 69 / E_BARRIER_INCOMPLETE by
// supervisor.go:694-697, so an operator cannot tell "the object store would not
// take the final LTX" from "the writeback cache did not drain in time" without
// reading the prose message. See finding F-4.
func TestExitSignalsDistinguishTheTerminalConditions(t *testing.T) {
	type signal struct {
		exit int
		code string
	}
	run := func(t *testing.T, mutate func(*Supervisor, *countingVolume), sig chan os.Signal) signal {
		t.Helper()
		vol := newCountingVolume()
		sup := newCloseoutSup(t, testSpec(), vol, &fakeCP{}, &fakeReplicator{}, newSharedFencer())
		if mutate != nil {
			mutate(sup, vol)
		}
		f := sup.Run(context.Background(), sig)
		return signal{exit: f.Exit, code: f.ErrCode}
	}

	got := map[string]signal{}

	got["clean stop"] = run(t, nil, stopAfter(120*time.Millisecond))

	got["lease loss"] = run(t, func(s *Supervisor, _ *countingVolume) {
		s.Deps.CP = &fakeCP{renewErr: &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "moved past"}}
	}, make(chan os.Signal))

	got["replication failure"] = run(t, func(s *Supervisor, _ *countingVolume) {
		s.Deps.Replicator = &fakeReplicator{syncErr: errors.New("object store refused the final LTX")}
	}, stopAfter(120*time.Millisecond))

	got["durability timeout"] = run(t, func(_ *Supervisor, v *countingVolume) {
		v.barrier = func(context.Context) (BarrierResult, error) {
			return BarrierResult{}, context.DeadlineExceeded
		}
	}, stopAfter(120*time.Millisecond))

	got["corruption"] = run(t, func(_ *Supervisor, v *countingVolume) {
		v.integrity = errors.New("database disk image is malformed")
	}, make(chan os.Signal))

	got["local-disk loss"] = run(t, func(s *Supervisor, _ *countingVolume) {
		// The state dir cannot be created: a file already occupies the path.
		if err := os.MkdirAll(filepath.Dir(s.Paths.StateDir), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(s.Paths.StateDir, []byte("not a directory"), 0o600); err != nil {
			t.Fatal(err)
		}
	}, make(chan os.Signal))

	want := map[string]signal{
		"clean stop":          {CodeOK, ""},
		"lease loss":          {CodeFenced, ErrCodeFencedOutOfBand},
		"replication failure": {CodeBarrierIncomplete, ErrCodeBarrierIncomplete},
		"durability timeout":  {CodeBarrierIncomplete, ErrCodeBarrierIncomplete},
		"corruption":          {CodeRestoreFailed, ErrCodeRestoreIntegrity},
		"local-disk loss":     {CodeRefused, ErrCodeRestoreFailed},
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = exit %d/%s, want exit %d/%s", name, got[name].exit, got[name].code, w.exit, w.code)
		}
	}

	// The collision, stated rather than implied.
	if got["replication failure"] == got["durability timeout"] {
		t.Logf("documented gap F-4: replication failure and durability timeout are both "+
			"exit %d/%s and cannot be told apart from the exit signal",
			got["durability timeout"].exit, got["durability timeout"].code)
	} else {
		t.Log("replication failure and durability timeout now differ — finding F-4 is fixed")
	}

	// Every distinct condition that IS distinguishable must stay distinguishable.
	seen := map[signal]string{}
	for _, name := range []string{"clean stop", "lease loss", "durability timeout", "corruption", "local-disk loss"} {
		if prev, dup := seen[got[name]]; dup {
			t.Errorf("%s and %s share exit %d/%s", prev, name, got[name].exit, got[name].code)
		}
		seen[got[name]] = name
	}
}

// TestAFailedSessionPurgeRefusesTheMount is PLO-362's fail-closed half: "if the
// cleanup fails, refuse to mount (exit 67-class)". A worker that could not
// prove the sweep happened must not serve the filesystem, because a dead
// writer's POSIX lock would silently block the live one for 25 minutes.
func TestAFailedSessionPurgeRefusesTheMount(t *testing.T) {
	vol := newCountingVolume()
	vol.purgeErr = errors.New("clean session 7: database is locked")
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), make(chan os.Signal))
	if f.Exit != CodeRestoreFailed {
		t.Fatalf("exit = %d (%v), want %d (the 67 class)", f.Exit, f.Err, CodeRestoreFailed)
	}
	if exists(t, sup.Paths.ReadyPath()) {
		t.Error("the worker published readiness despite a failed session sweep")
	}
	// The lease goes back so the Agent does not wait a full TTL for a mount
	// that never happened.
	if cp.released != "restore_failed" {
		t.Errorf("released with reason %q, want \"restore_failed\"", cp.released)
	}
}

// TestTheSessionSweepRunsBeforeTheFilesystemIsServed pins the ordering PLO-362
// depends on and that plori_fence.go states as a precondition: the sweep is
// total, so it must happen before this process opens its own session or it
// would delete its own row along with the dead writer's.
func TestTheSessionSweepRunsBeforeTheFilesystemIsServed(t *testing.T) {
	vol := newCountingVolume()
	sup := newCloseoutSup(t, testSpec(), vol, &fakeCP{}, &fakeReplicator{}, newSharedFencer())

	if f := sup.Run(context.Background(), stopAfter(120*time.Millisecond)); f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v)", f.Exit, f.Err)
	}
	order := vol.order()
	purgeAt := -1
	for i, c := range order {
		if c == "purge_sessions" {
			purgeAt = i
			break
		}
	}
	if purgeAt < 0 {
		t.Fatalf("the session sweep never ran: %v", order)
	}
	// Serve is what opens this process's own session (cmd/plori_mount.go:399);
	// the supervisor launches it only after start() returns, and the sweep is
	// inside start(). The barrier is the first thing the run loop records, so
	// "purge before any barrier" is the observable form of that ordering.
	for _, c := range order[:purgeAt] {
		if c == "barrier" {
			t.Fatalf("a barrier ran before the session sweep: %v", order)
		}
	}
}
