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
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// The shipped fencing arithmetic (adr.md §4 B3): TTL 2 m, renew 20 s,
// write-stop margin 45 s. Every number in this file is derived from those three
// and from the two measurements in benchmark-real-node.md §5, so a change to
// either shows up here as a failing expectation rather than as a silent drift.
const (
	prodTTL    = 2 * time.Minute
	prodRenew  = 20 * time.Second
	prodMargin = 45 * time.Second
	// prodMaxEarly is TTL - margin - 2*renew: how much earlier than the margin
	// the ordered stop may begin while still needing two consecutive renewal
	// failures to trigger.
	prodMaxEarly = 35 * time.Second
	// prodBudget is margin + prodMaxEarly: the time the ordered stop is
	// guaranteed for its drain, and what the backlog cap is sized against.
	prodBudget = 80 * time.Second
)

// drainSup builds a supervisor wired to the production fencing arithmetic, with
// the fake volume it will read the backlog from. It stops short of Run: these
// tests are about the arithmetic, and driving it through a real loop would test
// the tickers instead.
func drainSup(t *testing.T) (*Supervisor, *fakeVolume) {
	t.Helper()
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(prodRenew)
	spec.WriteStopMargin = Duration(prodMargin)
	spec.LeaseExpiresAt = time.Now().UTC().Add(prodTTL)
	vol := healthyVolume()
	sup := newSup(t, spec, &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(spec.LeaseExpiresAt, prodMargin, time.Now())
	sup.setLeaseTTL(prodTTL)
	return sup, vol
}

// The model learns from a barrier that actually drained something, and only
// from one.
//
// The second half is the PLO-346 trap stated as a test. Wave 2 measured
// max_flush_time at p95 120 ms and called it the number to size the margin
// against; every one of those stops had zero pending blocks
// (benchmark-real-node.md §5), so what it measured was the barrier's own
// bookkeeping. A model that folded those samples in would learn that a block
// costs ~120 ms whenever the mount is quiet, and would then be wrong in both
// directions at once — too pessimistic idle, too optimistic under load.
func TestTheDrainModelLearnsOnlyFromABarrierThatDrainedSomething(t *testing.T) {
	m := NewDrainModel(DefaultDrainPerBlock)
	if got := m.PerBlock(); got != DefaultDrainPerBlock {
		t.Fatalf("seed = %s, want %s", got, DefaultDrainPerBlock)
	}
	if m.Samples() != 0 {
		t.Fatalf("a seeded model has no samples, got %d", m.Samples())
	}

	// An empty-queue barrier: 120 ms of fixed cost, 0 blocks. Ignored.
	m.Observe(0, 120*time.Millisecond)
	// A near-empty one, below minDrainSample. Also ignored: 120 ms / 2 blocks
	// would teach 60 ms a block, which is the overhead, not the work.
	m.Observe(2, 120*time.Millisecond)
	if m.Samples() != 0 || m.PerBlock() != DefaultDrainPerBlock {
		t.Fatalf("shallow barriers moved the model: samples=%d per_block=%s", m.Samples(), m.PerBlock())
	}

	// The real measurement from the production node: 345 blocks in 10,724 ms.
	m.Observe(345, 10724*time.Millisecond)
	if m.Samples() != 1 {
		t.Fatalf("samples = %d, want 1", m.Samples())
	}
	// 10724/345 = 31.08 ms. The first real sample replaces the seed outright
	// rather than being averaged into it — the seed is another node's number.
	if got := m.PerBlock(); got < 31*time.Millisecond || got > 32*time.Millisecond {
		t.Fatalf("per_block = %s, want ~31ms (10,724 ms / 345 blocks)", got)
	}
	if rate := m.RatePerSecond(); rate < 32 || rate > 33 {
		t.Fatalf("rate = %.2f blocks/s, want ~32.2", rate)
	}

	// A slower barrier moves it, but not all the way: one bad minute must not
	// move the stop instant by minutes.
	before := m.PerBlock()
	m.Observe(100, 20*time.Second) // 200 ms a block
	after := m.PerBlock()
	if after <= before || after >= 200*time.Millisecond {
		t.Fatalf("per_block = %s, want strictly between %s and 200ms", after, before)
	}
}

// The ordered stop starts earlier as the backlog grows. This is threat-model.md
// §7.5's third instant: before PLO-383 the stop began AT the margin, which
// reserves a fixed tail for a drain whose length is not fixed.
func TestTheStopStartsEarlierAsTheBacklogGrows(t *testing.T) {
	sup, vol := drainSup(t)
	marginAt := sup.deadline.StopBy(0)

	for _, tc := range []struct {
		name    string
		pending uint64
		want    time.Duration
	}{
		{"an empty queue is exactly the old trigger", 0, 0},
		{"100 blocks at the seeded 32 ms", 100, 3200 * time.Millisecond},
		{"the profile ceiling still fits inside the lease", DefaultMaxStagingBacklog, 32768 * time.Millisecond},
		{"a backlog past the ceiling is clamped, not obeyed", 4000, prodMaxEarly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vol.pending.Store(tc.pending)
			if got := sup.stopEarliness(); got != tc.want {
				t.Fatalf("earliness at %d blocks = %s, want %s", tc.pending, got, tc.want)
			}
			at := sup.deadline.StopBy(sup.stopEarliness())
			if at.After(marginAt) {
				t.Fatalf("the stop instant moved LATER than the bare margin (%s vs %s)", at, marginAt)
			}
			if want := marginAt.Add(-tc.want); !at.Equal(want) {
				t.Fatalf("stop instant = %s, want %s", at, want)
			}
		})
	}
}

// The projection may never pull the stop in so far that a mount which is
// renewing normally tears itself down. The bound is stated in renewals: two
// consecutive failures, which is what the bare margin required before this
// change and what the fencing arithmetic in adr.md §4 B3 is sized for.
func TestTheEarliestStopStillNeedsTwoMissedRenewals(t *testing.T) {
	sup, vol := drainSup(t)
	vol.pending.Store(1_000_000) // a projection far past the whole lease

	if got := sup.maxStopEarliness(); got != prodMaxEarly {
		t.Fatalf("max earliness = %s, want %s (TTL - margin - 2*renew)", got, prodMaxEarly)
	}
	renewedAt := time.Now()
	sup.deadline.Update(time.Now().UTC().Add(prodTTL), prodMargin, renewedAt)

	earliest := sup.deadline.StopBy(sup.stopEarliness())
	gap := earliest.Sub(renewedAt)
	if gap < 2*prodRenew {
		t.Fatalf("the earliest stop is %s after a successful renewal, want at least %s", gap, 2*prodRenew)
	}
	if gap > 2*prodRenew+time.Second {
		t.Fatalf("the earliest stop is %s after a successful renewal, want ~%s", gap, 2*prodRenew)
	}

	// A lease with no room to spare degrades to the old behaviour rather than
	// to something clever.
	sup.setLeaseTTL(prodMargin)
	if got := sup.maxStopEarliness(); got != 0 {
		t.Fatalf("earliness with no room = %s, want 0", got)
	}
	if got := sup.stopEarliness(); got != 0 {
		t.Fatalf("stop earliness with no room = %s, want 0", got)
	}
}

// The cap is the other half of the same mechanism: the projection says when to
// start stopping, the cap keeps that instant inside the lease. It moves with
// the measured rate, and it never exceeds the profile ceiling.
func TestTheBacklogCapFollowsTheMeasuredDrainRate(t *testing.T) {
	sup, _ := drainSup(t)
	if got := sup.drainBudget(); got != prodBudget {
		t.Fatalf("budget = %s, want %s (margin + max earliness)", got, prodBudget)
	}

	sup.retuneBacklog()
	if got := sup.vol.(*fakeVolume).lastCap(); got != DefaultMaxStagingBacklog {
		// 80 s / 32 ms = 2,500 blocks, so the ceiling is what binds on a
		// healthy node — the cap costs nothing there.
		t.Fatalf("cap at the seeded rate = %d, want the ceiling %d", got, DefaultMaxStagingBacklog)
	}

	for _, tc := range []struct {
		name    string
		blocks  uint64
		elapsed time.Duration
		want    int64
	}{
		{"250 ms a block: the time term binds", 400, 100 * time.Second, 320},
		{"1 s a block: tighter still", 100, 100 * time.Second, 80},
		{"back to fast: the ceiling binds again", 1000, 5 * time.Second, DefaultMaxStagingBacklog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Drive the model to the rate under test rather than nudging it:
			// the EMA is deliberately slow, and this test is about the cap.
			sup.drain = NewDrainModel(tc.elapsed / time.Duration(tc.blocks))
			sup.observeDrain(tc.blocks, tc.elapsed)
			if got := sup.vol.(*fakeVolume).lastCap(); got != tc.want {
				t.Fatalf("cap = %d, want %d (per_block %s, budget %s)",
					got, tc.want, sup.drain.PerBlock(), sup.drainBudget())
			}
		})
	}

	// A node so slow that not even one block fits still stages one. A cap of
	// zero would throttle every write to a synchronous round trip forever and
	// call it back-pressure.
	m := NewDrainModel(10 * time.Minute)
	if got := m.CapForBudget(prodBudget, DefaultMaxStagingBacklog); got != 1 {
		t.Fatalf("cap on an impossibly slow node = %d, want 1", got)
	}
}

// PLO-346's arithmetic, flipped: the same numbers, and what the mechanism does
// with them.
//
// benchmark-real-node.md §5 measured 1,008 staged blocks draining in 595 s
// against the 45 s margin — 13.2x — and attributed it to ~590 ms of local
// per-block work. It is not that (see TestADeepBacklogDrainsWithoutWaitingForTheSweep
// in pkg/chunk: the passive drain was quantised by a one-minute re-queue sweep),
// but the mechanism here has to hold at EITHER rate, because a measured rate is
// exactly what it does not get to assume.
func TestThePLO346BacklogCannotHappenAtEitherMeasuredRate(t *testing.T) {
	const observedBacklog = 1008
	// The ADR's estimate of a full staging area at 4 MiB blocks and a 2 GiB
	// cache: nothing bounded the backlog to a thousand.
	const fullStagingArea = 16000

	for _, tc := range []struct {
		name     string
		perBlock time.Duration
		wantCap  int64
		// wantHarmless says whether a backlog as deep as the one PLO-346
		// measured is still allowed at this rate. It is allowed exactly when
		// it drains inside the margin, which is the whole point: the cap is
		// not a number, it is a time bound expressed in blocks.
		wantHarmless bool
	}{
		// The run's own barrier: 10,724 ms for ~345 blocks. At this rate 1,008
		// blocks drain in ~31 s, inside the 45 s margin, so the backlog PLO-346
		// measured is not a problem at all -- which is the point the run missed.
		{"the measured barrier rate", 31 * time.Millisecond, 1024, true},
		// The run's headline reading of the passive drain. At this rate the
		// same backlog is 595 s, and the cap makes it unreachable.
		{"the reported passive rate", 590 * time.Millisecond, 135, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewDrainModel(tc.perBlock)
			limit := m.CapForBudget(prodBudget, DefaultMaxStagingBacklog)
			if limit != tc.wantCap {
				t.Fatalf("cap = %d, want %d", limit, tc.wantCap)
			}
			// The property the constant margin did not have: whatever the cap
			// admits drains inside the stop's budget, at whatever rate this
			// node actually runs.
			if drain := m.Project(uint64(limit)); drain > prodBudget {
				t.Fatalf("a backlog at the cap (%d blocks) projects %s, past the %s budget", limit, drain, prodBudget)
			}
			if limit >= fullStagingArea {
				t.Fatalf("cap = %d: a full staging area is still reachable", limit)
			}
			// And the 13.2x failure itself: 1,008 blocks either drain inside
			// the margin, or they can never accumulate.
			drain, reachable := m.Project(observedBacklog), limit >= observedBacklog
			if reachable != tc.wantHarmless {
				t.Fatalf("%d blocks reachable = %v, want %v (cap %d)", observedBacklog, reachable, tc.wantHarmless, limit)
			}
			if reachable && drain > prodMargin {
				t.Fatalf("%d blocks are reachable and project %s, past the %s margin", observedBacklog, drain, prodMargin)
			}
			if !reachable && drain <= prodMargin {
				t.Fatalf("%d blocks project %s, inside the margin, so capping them below %d costs throughput for nothing",
					observedBacklog, drain, limit)
			}
		})
	}
}

// health.json carries the projection, the rate and the cap, because the CSI
// plugin cannot compute any of them: the SIGKILL deadline it enforces is
// write_stop_margin + this, and an operator needs to see a mount whose backlog
// has outgrown its stop window BEFORE the stop.
func TestHealthPublishesTheProjectedDrain(t *testing.T) {
	sup, vol := drainSup(t)
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	vol.pending.Store(500)
	sup.retuneBacklog()
	sup.writeHealth()

	raw, err := os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"projected_drain_seconds", "drain_rate_blocks_per_s", "drain_samples", "staging_backlog_cap"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("health.json has no %q; the plugin's kill deadline reads it", key)
		}
	}
	// 500 blocks at the seeded 32 ms.
	if v := got["projected_drain_seconds"].(float64); v < 15.9 || v > 16.1 {
		t.Fatalf("projected_drain_seconds = %v, want ~16 (500 blocks x 32 ms)", v)
	}
	if v := got["drain_rate_blocks_per_s"].(float64); v < 31 || v > 32 {
		t.Fatalf("drain_rate_blocks_per_s = %v, want ~31.25", v)
	}
	if v := got["drain_samples"].(float64); v != 0 {
		t.Fatalf("drain_samples = %v, want 0 while the model is still the seed", v)
	}
	if v := got["staging_backlog_cap"].(float64); v != DefaultMaxStagingBacklog {
		t.Fatalf("staging_backlog_cap = %v, want %d", v, DefaultMaxStagingBacklog)
	}
}

// A stop that runs out of time still exits 69, and now says by how much. "The
// margin was not enough" is only actionable next to the number that would have
// been (benchmark-real-node.md §5 had to be a whole benchmark run to produce
// one).
func TestAnIncompleteStopReportsTheMeasuredShortfall(t *testing.T) {
	sup, vol := drainSup(t)
	vol.pending.Store(900)
	vol.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{PendingBlocks: 900}, errors.New("context deadline exceeded")
	}

	got := sup.shutdown(context.Background(), ReasonShutdown)
	if got.Exit != CodeBarrierIncomplete {
		t.Fatalf("exit = %d, want %d (%v)", got.Exit, CodeBarrierIncomplete, got.Err)
	}
	msg := got.Err.Error()
	for _, want := range []string{"900 blocks staged at the start", "still staged", "blocks/s", "budget"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("exit message %q does not carry %q", msg, want)
		}
	}
}

// A completed stop barrier is a drain sample like any other, and the best one
// there is: it is the exact operation the projection exists to predict.
func TestTheStopBarrierFeedsTheDrainModel(t *testing.T) {
	sup, vol := drainSup(t)
	vol.pending.Store(200)
	var barrierAt time.Time
	vol.barrier = func(context.Context) (BarrierResult, error) {
		barrierAt = time.Now()
		time.Sleep(20 * time.Millisecond)
		// The drain finished, so the backlog the model measures is gone.
		vol.pending.Store(0)
		return BarrierResult{BarrierAt: barrierAt.UTC()}, nil
	}

	if got := sup.shutdown(context.Background(), ReasonShutdown); got.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", got.Exit, got.Err)
	}
	if sup.drain.Samples() != 1 {
		t.Fatalf("samples = %d, want 1: the stop's own barrier is a measurement", sup.drain.Samples())
	}
	// 20 ms over 200 blocks is 100 µs a block, far under the seed.
	if got := sup.drain.PerBlock(); got >= DefaultDrainPerBlock {
		t.Fatalf("per_block = %s, want the measured value, not the seed", got)
	}
}

// A Supervisor that never ran its startup chain still writes health.json — on
// every failure path, and in the tests that build one directly. A nil model
// there has to read as "nothing measured yet", not crash the process while it
// is reporting why it is stopping. (Found by merging PLO-322's fork PR, whose
// credential tests call writeHealth on a bare Supervisor.)
func TestAnUnstartedSupervisorStillWritesHealth(t *testing.T) {
	var m *DrainModel
	if got := m.PerBlock(); got != DefaultDrainPerBlock {
		t.Fatalf("nil model per_block = %s, want the seed %s", got, DefaultDrainPerBlock)
	}
	if m.Samples() != 0 {
		t.Fatalf("nil model samples = %d, want 0", m.Samples())
	}
	m.Observe(100, time.Second) // must not panic and must not record
	if m.Samples() != 0 {
		t.Fatalf("nil model recorded a sample")
	}
	if got := m.Project(10); got != 10*DefaultDrainPerBlock {
		t.Fatalf("nil model projection = %s, want %s", got, 10*DefaultDrainPerBlock)
	}
	if got := m.CapForBudget(prodBudget, DefaultMaxStagingBacklog); got != DefaultMaxStagingBacklog {
		t.Fatalf("nil model cap = %d, want the ceiling", got)
	}

	sup, _ := drainSup(t)
	sup.drain = nil
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sup.writeHealth()
	if _, err := os.Stat(sup.Paths.HealthPath()); err != nil {
		t.Fatalf("an unstarted supervisor wrote no health file: %s", err)
	}
}
