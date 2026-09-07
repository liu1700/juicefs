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
	"syscall"
	"testing"
	"time"
)

// Health publishes cheap totals. The optional trash report pairs its breakdown with
// totals from the same background observation; neither health ticks nor shutdown
// may start a synchronous walk on the lease loop.

// runningSup starts a supervisor and stops it when the test ends.
func runningSup(t *testing.T, sup *Supervisor) {
	t.Helper()
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })
}

// A mount publishes the volume it actually has from its first health.json, not
// from its first /usage report. This is PLO-427's regression: the figure has to
// be in the file before any report has happened, because on a five-minute
// report cadence "after the first report" is most of a short Agent session.
func TestHealthJSONCarriesTheVolumesUsageBeforeTheFirstReport(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 68747263, Inodes: 35}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	var first Health
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		if ok {
			first = h
		}
		return ok
	}, "health.json was never written")

	// The claim is about a mount that has not reported yet, so say so rather
	// than assume it: the first report is fifteen renews away and the first
	// health write is one.
	if n := len(cp.reportedUsages()); n != 0 {
		t.Fatalf("%d usage reports landed before the first health.json; this test no longer proves what it claims", n)
	}
	if first.UsedBytes != 68747263 || first.UsedInodes != 35 {
		t.Errorf("first health.json = %d B / %d inodes, want 68747263 / 35: publishing zero for a volume that has data is PLO-427",
			first.UsedBytes, first.UsedInodes)
	}
}

// The two publications track the same volume. A figure that moves under a
// running mount reaches the control-plane and health.json, and they agree on
// it — one accessor, so there is nothing for them to disagree about.
func TestHealthJSONAndTheUsageReportCarryTheSameFigure(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 4 << 20, Inodes: 7}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	waitFor(t, 20*time.Second, func() bool { return len(cp.reportedUsages()) > 0 },
		"the worker never reported usage")

	// The Agent writes.
	vol.setUsage(Usage{Bytes: 96 << 20, Inodes: 341}, nil)

	waitFor(t, 20*time.Second, func() bool {
		u := cp.reportedUsages()
		return u[len(u)-1] == Usage{Bytes: 96 << 20, Inodes: 341}
	}, "the control-plane was never told the volume had grown")
	waitFor(t, 20*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.UsedBytes == 96<<20 && h.UsedInodes == 341
	}, "health.json never carried the figure the control-plane was told")
}

func TestHealthTicksNeverStartATrashWalk(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 40 << 20, Inodes: 61, TrashKnown: true, TrashBytes: 8 << 20, TrashInodes: 12}, nil)
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	ctx := context.Background()
	for i := 0; i < 36; i++ {
		u, ok := sup.usageTotals(ctx)
		if !ok || u.Bytes != 40<<20 || u.TrashKnown {
			t.Fatalf("health totals: %+v, ok=%v", u, ok)
		}
	}
	reads, walks := vol.usageCounts()
	if reads != 36 || walks != 0 {
		t.Fatalf("reads=%d walks=%d, want 36/0", reads, walks)
	}
	results := make(chan usageObservation, 1)
	sup.startUsageObservation(ctx, results)
	// Even a completed observation stays in flight until the loop handles its result.
	for i := 0; i < 36; i++ {
		sup.startUsageObservation(ctx, results)
	}
	select {
	case observed := <-results:
		if !observed.usage.TrashKnown {
			t.Fatal("background observation omitted known trash")
		}
		sup.finishUsageObservation(ctx, observed)
	case <-time.After(time.Second):
		t.Fatal("observation did not finish")
	}
	if _, walks := vol.usageCounts(); walks != 1 {
		t.Fatalf("walks=%d, want one in-flight observation", walks)
	}
	// The next health total must not pair the old breakdown with a new total.
	u, ok := sup.usageTotals(ctx)
	if !ok || u.TrashKnown || u.TrashBytes != 0 {
		t.Fatalf("health reused old breakdown: %+v", u)
	}
	sup.startUsageObservation(ctx, results)
	select {
	case observed := <-results:
		sup.finishUsageObservation(ctx, observed)
	case <-time.After(time.Second):
		t.Fatal("next observation did not finish")
	}
	if _, walks := vol.usageCounts(); walks != 2 {
		t.Fatalf("walks=%d, want second observation", walks)
	}
}

func TestSlowTrashObservationDoesNotDelayRenewal(t *testing.T) {
	vol := healthyVolume()
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	vol.usageStarted = started
	vol.usageBlock = unblock
	spec := testSpec()
	cp := &fakeCP{}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("trash observation never started")
	}
	before := len(cp.renewRequests())
	waitFor(t, 2*time.Second, func() bool { return len(cp.renewRequests()) >= before+3 },
		"renewal stopped behind a trash observation")
	close(unblock)
}

func TestStopCancelsSlowTrashObservation(t *testing.T) {
	vol := healthyVolume()
	started, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
	vol.usageStarted, vol.usageCanceled = started, canceled
	vol.usageBlock = make(chan struct{})
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	stop, done := make(chan os.Signal, 1), make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("trash observation never started")
	}
	stop <- syscall.SIGTERM
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not cancel the trash observation")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func TestBlockedTrashObservationDoesNotDelayDeadlineFence(t *testing.T) {
	vol := healthyVolume()
	started, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
	vol.usageStarted, vol.usageCanceled = started, canceled
	vol.usageBlock = make(chan struct{})
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().UTC().Add(1200 * time.Millisecond)
	spec.WriteStopMargin = Duration(900 * time.Millisecond)
	spec.LeaseRenewInterval = Duration(30 * time.Millisecond)
	sup := newSup(t, spec, &fakeFS{vol: vol}, &fakeCP{renewErr: context.DeadlineExceeded}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	observeCtx, cancelObserve := context.WithCancel(context.Background())
	defer cancelObserve()
	sup.startUsageObservation(observeCtx, make(chan usageObservation, 1))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("trash observation never started")
	}
	start := time.Now()
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	f := waitFatal(t, done, 2*time.Second, "deadline guard waited behind trash observation")
	if f.Exit != CodeFenced && f.Exit != CodeBarrierIncomplete {
		t.Fatalf("exit = %d (%v), want fenced or incomplete barrier", f.Exit, f.Err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("deadline fence took %s while observation was blocked", elapsed)
	}
	if !vol.Fenced() {
		t.Fatal("deadline guard did not fence the volume")
	}
	cancelObserve()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("blocked observation did not stop after cancellation")
	}
}

// A volume nobody can measure keeps its last known figure. Zero is a real
// answer — an Agent that has written nothing — so publishing it for a failed
// reading would tell the plugin's gauge, and the operator reading it beside
// quota_exhausted, that a full volume had emptied itself.
func TestAnUnreadableUsageKeepsTheLastFigureRatherThanPublishingZero(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 12 << 20, Inodes: 9}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.UsedBytes == 12<<20
	}, "health.json never carried the volume's usage")

	vol.setUsage(Usage{}, errors.New("metadata engine is gone"))
	// Long enough to cross a report tick as well as many health writes: the
	// report is every fifteenth renew and this waits for twenty more.
	before := len(cp.renewRequests())
	waitFor(t, 20*time.Second, func() bool { return len(cp.renewRequests()) >= before+20 },
		"the worker stopped renewing")

	if h := readHealth(t, sup); h.UsedBytes != 12<<20 || h.UsedInodes != 9 {
		t.Errorf("health.json = %d B / %d inodes after the readings started failing, want the last good 12582912 / 9",
			h.UsedBytes, h.UsedInodes)
	}
	for _, u := range cp.reportedUsages() {
		if u.Bytes == 0 {
			t.Error("a failed reading was reported to the control-plane as an empty volume")
		}
	}
}
