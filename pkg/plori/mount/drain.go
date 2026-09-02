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

// DrainModel is the live answer to "how long would the writeback backlog take
// to reach the object store".
//
// It exists because the write-stop margin is a constant and the drain is not.
// PLO-346 measured a stop against a deep queue on the production node shape and
// the constant lost by 13x, so the margin cannot be the whole of the reserved
// tail: the supervisor has to know, at every instant, how much work is standing
// between it and a durable stop. That number has exactly one honest source --
// the periodic barrier, which is a real drain of a real backlog and already
// runs every 60 s. Nothing here estimates from a formula; it divides a measured
// elapsed by a measured backlog and keeps an exponential average of the
// quotient.
//
// The average is over per-block cost rather than over total drain time because
// the backlog is what varies: two barriers 60 s apart may drain 4 blocks and
// 400, and only the quotient is comparable between them.
type DrainModel struct {
	mu sync.RWMutex
	// perBlock is the exponentially averaged cost of draining one staged block.
	perBlock time.Duration
	// samples is how many barriers have contributed. Reported so an operator
	// can tell a seeded model from a measured one.
	samples int
}

const (
	// drainEMAAlpha weights the newest sample. 0.3 reaches ~90% of a step
	// change in six barriers, i.e. about six minutes at the 60 s barrier
	// period -- fast enough to follow a node that has become slow, slow enough
	// that one unlucky barrier does not move the stop instant by minutes.
	drainEMAAlpha = 0.3
	// minDrainSample is the shallowest backlog a barrier may be measured
	// against. Below it the barrier's own fixed cost (24-168 ms measured with
	// an empty queue, benchmark-wave2-footprint.md §10) dominates the quotient
	// and the model would learn a per-block cost that is mostly overhead.
	minDrainSample = 8
)

// NewDrainModel seeds the model. The seed is used until a barrier drains at
// least minDrainSample blocks; it is the measured production-node figure
// (mountspec.DefaultDrainPerBlock) rather than zero, because a model that
// starts at zero projects a zero drain and would leave the very first deep
// backlog with exactly the bound PLO-346 refuted.
func NewDrainModel(seed time.Duration) *DrainModel {
	if seed <= 0 {
		seed = DefaultDrainPerBlock
	}
	return &DrainModel{perBlock: seed}
}

// Observe folds one completed barrier into the model: `blocks` were staged and
// not yet durable when it started, and it took `elapsed` to make them durable.
//
// A barrier that started with fewer than minDrainSample blocks is ignored
// rather than clamped: it carries no information about per-block cost, and
// feeding it in would drag the average toward the barrier's fixed overhead
// every time the mount is quiet -- which is the regime the wave-2 measurement
// was taken in, and the reason its number was a floor rather than a bound.
func (d *DrainModel) Observe(blocks uint64, elapsed time.Duration) {
	if d == nil || blocks < minDrainSample || elapsed <= 0 {
		return
	}
	sample := elapsed / time.Duration(blocks)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.samples == 0 {
		d.perBlock = sample
	} else {
		d.perBlock = time.Duration(drainEMAAlpha*float64(sample) + (1-drainEMAAlpha)*float64(d.perBlock))
	}
	if d.perBlock <= 0 {
		d.perBlock = time.Nanosecond
	}
	d.samples++
}

// PerBlock is the current estimate of one block's drain cost.
//
// A nil model is the seed. The type is total on purpose: health.json is written
// on failure paths and by callers that never ran the startup chain, and a
// supervisor that has not measured anything yet should read as "nothing
// measured yet" rather than crash while reporting why it is stopping.
func (d *DrainModel) PerBlock() time.Duration {
	if d == nil {
		return DefaultDrainPerBlock
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.perBlock
}

// Samples is how many barriers have been measured. Zero means the model is
// still the seed.
func (d *DrainModel) Samples() int {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.samples
}

// Project is how long `blocks` would take to drain at the current estimate. It
// is the raw projection, not clamped to anything: health.json publishes what
// the worker actually believes, and the two consumers clamp it for their own
// budgets (the stop instant here, the SIGKILL deadline in the CSI plugin).
func (d *DrainModel) Project(blocks uint64) time.Duration {
	if blocks == 0 {
		return 0
	}
	return time.Duration(blocks) * d.PerBlock()
}

// RatePerSecond is the estimate as blocks per second, which is the form an
// operator reads a dashboard in.
func (d *DrainModel) RatePerSecond() float64 {
	per := d.PerBlock()
	if per <= 0 {
		return 0
	}
	return float64(time.Second) / float64(per)
}

// CapForBudget is the deepest backlog that still drains inside `budget` at the
// current estimate, never above `ceiling`.
//
// This is the other half of the same mechanism: projecting the drain tells the
// supervisor when to start stopping, and capping the backlog is what keeps that
// instant inside the lease. Without the cap a backlog can grow until the
// projection exceeds the whole lease, and then there is no instant early enough
// -- which is precisely the state PLO-346 measured.
func (d *DrainModel) CapForBudget(budget time.Duration, ceiling int64) int64 {
	if ceiling <= 0 {
		return 0
	}
	if budget <= 0 {
		return ceiling
	}
	per := d.PerBlock()
	if per <= 0 {
		return ceiling
	}
	fits := int64(budget / per)
	if fits < 1 {
		// One block always gets to be staged. A cap of zero would make every
		// write synchronous forever on a node whose measured per-block cost
		// exceeds the whole budget, and a mount that cannot stage one block is
		// a mount that should be reported, not silently throttled to a halt.
		fits = 1
	}
	if fits > ceiling {
		return ceiling
	}
	return fits
}
