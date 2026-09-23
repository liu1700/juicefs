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

package meta

import (
	"sync/atomic"
	"syscall"
)

// volumeReservation is one metadata call's in-memory claim on the volume
// ceiling (Format.Capacity and Format.Inodes). A caller opts in by installing
// one in the request context; without it every path in this file falls back to
// the unchanged upstream behavior.
//
// The upstream ceiling check reads the counters, and the counters move only
// after the metadata transaction commits. Two operations can therefore both
// pass the check before either is counted. A claim closes that window: the
// check and the claim happen under volMu, the claim stays visible to every
// later check until the committed amount replaces it in newSpace, and whatever
// was not committed is released when the call returns.
//
// A claim is never persisted and never fed to updateStats or doFlushStats, so
// used-space reports, dumps and the replicated counters are unaffected.
//
// All fields are guarded by baseMeta.volMu.
type volumeReservation struct {
	// space and inodes are what the latest ceiling check of this call claimed
	// and has not yet converted or charged.
	space, inodes int64
	// charges are clone transactions claimed before they commit, keyed by the
	// first destination inode of the transaction. The key is allocated before
	// the transaction starts, so a retried attempt replaces its own charge.
	charges map[Ino]volumeCharge
}

type volumeCharge struct{ space, inodes int64 }

type volumeReservationKey struct{}

func newVolumeReservation() *volumeReservation { return &volumeReservation{} }

func withVolumeReservation(ctx Context, r *volumeReservation) Context {
	return ctx.WithValue(volumeReservationKey{}, r)
}

func volumeReservationFrom(ctx Context) *volumeReservation {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(volumeReservationKey{}).(*volumeReservation)
	return r
}

// Test seams, nil outside tests. They run inside the critical sections they
// are named after so a test can prove what those sections exclude.
var (
	volumeTransferTestHook atomic.Pointer[func()]
	refreshUsageTestHook   atomic.Pointer[func()] // after the remote read
)

func runVolumeTestHook(h *atomic.Pointer[func()]) {
	if f := h.Load(); f != nil {
		(*f)()
	}
}

func positivePart(v int64) int64 {
	if v > 0 {
		return v
	}
	return 0
}

// volumeFitsLocked reports whether space and inodes fit under the ceiling
// after committed usage, unflushed deltas and every outstanding claim.
// The caller holds volMu.
func (m *baseMeta) volumeFitsLocked(space, inodes int64) bool {
	format := m.getFormat()
	if space > 0 && format.Capacity > 0 &&
		atomic.LoadInt64(&m.usedSpace)+atomic.LoadInt64(&m.newSpace)+m.pendingSpace+space > int64(format.Capacity) {
		return false
	}
	if inodes > 0 && format.Inodes > 0 &&
		atomic.LoadInt64(&m.usedInodes)+atomic.LoadInt64(&m.newInodes)+m.pendingInodes+inodes > int64(format.Inodes) {
		return false
	}
	return true
}

// reserveVolume checks and claims the positive parts of space and inodes for
// the call owning r.
//
// A call checks the ceiling once per transaction attempt. A retried attempt
// rechecks the same operation after the earlier attempt rolled back, so the new
// claim replaces the earlier one rather than adding to it: a call can never be
// refused by its own stale claim.
func (m *baseMeta) reserveVolume(r *volumeReservation, space, inodes int64) bool {
	space, inodes = positivePart(space), positivePart(inodes)
	m.volMu.Lock()
	defer m.volMu.Unlock()
	m.pendingSpace -= r.space
	m.pendingInodes -= r.inodes
	r.space, r.inodes = 0, 0
	if !m.volumeFitsLocked(space, inodes) {
		return false
	}
	m.pendingSpace += space
	m.pendingInodes += inodes
	r.space, r.inodes = space, inodes
	return true
}

// dropVolumeClaim releases the unconverted claim of the latest ceiling check,
// used when a later check of the same call refuses the operation.
func (m *baseMeta) dropVolumeClaim(r *volumeReservation) {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	m.pendingSpace -= r.space
	m.pendingInodes -= r.inodes
	r.space, r.inodes = 0, 0
}

// chargeVolume claims one clone transaction before it commits. It draws on the
// call's clone preflight claim first and checks only the shortfall against the
// ceiling. A refusal is ENOSPC without the volume quota hook: the transaction
// rolls back, nothing is left to count, and a partially completed clone must
// fail rather than be retried by the admission wrapper.
func (m *baseMeta) chargeVolume(ctx Context, key Ino, space, inodes int64) syscall.Errno {
	r := volumeReservationFrom(ctx)
	if r == nil {
		return 0
	}
	space, inodes = positivePart(space), positivePart(inodes)
	m.volMu.Lock()
	defer m.volMu.Unlock()
	if old, ok := r.charges[key]; ok {
		// An earlier attempt of this transaction rolled back; its charge is
		// unused capacity of this call again.
		delete(r.charges, key)
		r.space += old.space
		r.inodes += old.inodes
	}
	fromSpace, fromInodes := min(space, r.space), min(inodes, r.inodes)
	needSpace, needInodes := space-fromSpace, inodes-fromInodes
	if !m.volumeFitsLocked(needSpace, needInodes) {
		return syscall.ENOSPC
	}
	r.space -= fromSpace
	r.inodes -= fromInodes
	m.pendingSpace += needSpace
	m.pendingInodes += needInodes
	if r.charges == nil {
		r.charges = make(map[Ino]volumeCharge)
	}
	r.charges[key] = volumeCharge{space, inodes}
	return 0
}

// commitVolume counts space and inodes that a metadata transaction committed.
// With a reservation it converts the named charges, then the call's check
// claim, into newSpace/newInodes in the same critical section, so a check
// never sees the amount in neither place. Growth beyond the claim is still
// counted and recorded in unreservedSpace/unreservedInodes.
func (m *baseMeta) commitVolume(ctx Context, keys []Ino, space, inodes int64) {
	r := volumeReservationFrom(ctx)
	if r == nil {
		m.en.updateStats(space, inodes)
		return
	}
	m.volMu.Lock()
	defer m.volMu.Unlock()
	m.en.updateStats(space, inodes)
	var chargedSpace, chargedInodes int64
	for _, key := range keys {
		if c, ok := r.charges[key]; ok {
			delete(r.charges, key)
			chargedSpace += c.space
			chargedInodes += c.inodes
		}
	}
	m.pendingSpace -= chargedSpace
	m.pendingInodes -= chargedInodes
	if rest := positivePart(space) - chargedSpace; rest > 0 {
		covered := min(rest, r.space)
		r.space -= covered
		m.pendingSpace -= covered
		m.unreservedSpace += rest - covered
	}
	if rest := positivePart(inodes) - chargedInodes; rest > 0 {
		covered := min(rest, r.inodes)
		r.inodes -= covered
		m.pendingInodes -= covered
		m.unreservedInodes += rest - covered
	}
}

// releaseVolumeReservation drops whatever the call claimed and did not commit.
// The owner calls it once the metadata call has returned, before any wait.
func (m *baseMeta) releaseVolumeReservation(r *volumeReservation) {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	m.pendingSpace -= r.space
	m.pendingInodes -= r.inodes
	r.space, r.inodes = 0, 0
	for key, c := range r.charges {
		m.pendingSpace -= c.space
		m.pendingInodes -= c.inodes
		delete(r.charges, key)
	}
}

// claimVolumeGrowth claims growth the engine makes on the call's behalf
// without a ceiling check of its own, such as a new hourly trash bucket.
// settleVolumeGrowth must follow once the growth committed or failed.
func (m *baseMeta) claimVolumeGrowth(space, inodes int64) bool {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	if !m.volumeFitsLocked(space, inodes) {
		return false
	}
	m.pendingSpace += space
	m.pendingInodes += inodes
	return true
}

func (m *baseMeta) settleVolumeGrowth(space, inodes int64, committed bool) {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	if committed {
		m.en.updateStats(space, inodes)
	}
	m.pendingSpace -= space
	m.pendingInodes -= inodes
}

// volumeClaimed is committed usage, unflushed deltas and outstanding claims,
// read as one consistent value.
func (m *baseMeta) volumeClaimed() (space, inodes int64) {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	space = atomic.LoadInt64(&m.usedSpace) + atomic.LoadInt64(&m.newSpace) + m.pendingSpace
	inodes = atomic.LoadInt64(&m.usedInodes) + atomic.LoadInt64(&m.newInodes) + m.pendingInodes
	return
}
