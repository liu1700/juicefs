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
	"sync"
	"time"
)

// Deadline converts the control-plane's wall-clock lease expiry into this
// process's monotonic clock, once, at receipt.
//
// The conversion is the whole point. threat-model.md §7.2: a writer that is
// SIGSTOPped, or whose container is frozen and thawed, resumes believing it
// still holds the lease if it re-reads the wall clock. A Go time.Time captured
// with time.Now() carries a monotonic reading, and time.Since on it measures
// elapsed process time regardless of what happened to the wall clock — so the
// deadline is stored as "the monotonic instant at which the lease dies" and is
// never recomputed from a wall-clock subtraction afterwards.
//
// It is deliberately a value with a mutex rather than an atomic instant: the
// renew loop writes it and the write path reads it on every submission, and
// the read must never see a half-updated pair.
type Deadline struct {
	mu sync.RWMutex
	// expiry is a monotonic instant, derived once per renewal.
	expiry time.Time
	// margin is the tail of the lease reserved for flush plus the durability
	// barrier. New writes stop at expiry-margin.
	margin time.Time
	// wallExpiry is the deadline as the DATABASE measured it, kept only for
	// reporting; nothing decides on it.
	wallExpiry time.Time
}

// MaxClockJump is how far the wall clock may drift against the monotonic clock
// between renewals before the worker treats itself as fenced. mountspec.md
// §8 item 5 asks for exactly this: "treat a monotonic-clock jump beyond a
// threshold as a fence trip". The renew interval is 20 s today, so a full
// second of unexplained divergence per renewal is already far outside NTP
// slew and means the process was stopped or the clock was stepped.
const MaxClockJump = time.Second

// NewDeadline records the first deadline, from the MountSpec.
func NewDeadline(expiresAt time.Time, margin time.Duration, now time.Time) *Deadline {
	d := &Deadline{}
	d.Update(expiresAt, margin, now)
	return d
}

// Update converts a fresh wall-clock expiry to the monotonic clock. `now` must
// carry a monotonic reading (i.e. come from time.Now()); everything after this
// call compares against it monotonically, so a wall-clock step cannot move the
// deadline. The remaining lifetime is taken from the server's own arithmetic:
// mountspec.md §6 makes the write-stop margin the term that absorbs clock skew,
// so the worker does not add a second, uncoordinated safety term here.
func (d *Deadline) Update(expiresAt time.Time, margin time.Duration, now time.Time) {
	remaining := expiresAt.Sub(now.UTC())
	d.mu.Lock()
	defer d.mu.Unlock()
	d.expiry = now.Add(remaining)
	d.margin = d.expiry.Add(-margin)
	d.wallExpiry = expiresAt
}

// WriteAllowed reports whether the mount is still inside its write-stop
// margin. It is the supervisor's trigger to begin the ordered stop, NOT the
// per-submission check: the per-submission check threat-model.md:812-815 asks
// for lives in the metadata engine, which re-reads Expiry on every gated
// operation (meta.PloriSetWriteExpiry, PLO-323 F-5). Two instants, one
// mechanism: the margin stops the mount, the expiry stops the writes.
//
// Before F-5 this function was the only deadline check in the process and it
// ran on a one-second ticker, which is precisely the timer the requirement it
// cites forbids.
func (d *Deadline) WriteAllowed(now time.Time) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return now.Before(d.margin)
}

// Expiry is the monotonic instant at which the lease itself dies. It is what
// the metadata engine's write gate is armed with: after it, another writer may
// exist, so nothing may be committed — not even the drain.
func (d *Deadline) Expiry() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.expiry
}

// Expired reports whether the lease itself is gone, margin included.
func (d *Deadline) Expired(now time.Time) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return !now.Before(d.expiry)
}

// Remaining is how long until new writes must stop.
func (d *Deadline) Remaining(now time.Time) time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.margin.Sub(now)
}

// RemainingLease is how long until the lease itself dies. It bounds the
// shutdown sequence: a barrier that would outlive the lease must be abandoned
// rather than finished (crash-consistency Q7 / PLO-323 fault 4).
func (d *Deadline) RemainingLease(now time.Time) time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.expiry.Sub(now)
}

// WallExpiry is the control-plane's own deadline, for health.json only.
func (d *Deadline) WallExpiry() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.wallExpiry
}

// ClockJump measures how far the wall clock has moved relative to the
// monotonic clock between two time.Now() readings.
//
// A time.Time carrying a monotonic reading subtracts monotonically; stripping
// it with Round(0) forces the wall-clock subtraction. When the two disagree
// the wall clock was stepped, or the process was stopped and thawed, which is
// exactly the condition threat-model.md §7.2 says must be treated as a fence
// trip. BOTH arguments must come from time.Now(): a time.Time built by
// arithmetic on a stripped value has no monotonic reading and this returns 0.
func ClockJump(since, now time.Time) time.Duration {
	return clockJump(now.Sub(since), now.Round(0).Sub(since.Round(0)))
}

// clockJump is the arithmetic, split out because the monotonic clock cannot be
// stepped from a test: Go exposes no way to build a time.Time whose monotonic
// and wall readings disagree, so the wrapper above is exercised in the
// integration test and the arithmetic is exercised here.
func clockJump(monoElapsed, wallElapsed time.Duration) time.Duration {
	diff := wallElapsed - monoElapsed
	if diff < 0 {
		diff = -diff
	}
	return diff
}
