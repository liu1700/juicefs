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
	"fmt"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Every scenario here runs twice. The control arm uses no reservation, which
// is the upstream path: it must show the overshoot, proving the test really
// reaches the window. The reserved arm must refuse and stay under the ceiling.

type volresEngine struct {
	name  string
	reset bool
	open  func(t *testing.T) (Meta, error)
}

var volresSQLiteEngine = volresEngine{name: "sqlite3", open: func(t *testing.T) (Meta, error) {
	return newSQLMeta("sqlite3", filepath.Join(t.TempDir(), "volres.db"), testConfig())
}}

var volresRedisEngine = volresEngine{name: "redis", reset: true, open: func(t *testing.T) (Meta, error) {
	return newRedisMeta("redis", "127.0.0.1:6379/8", testConfig())
}}

// volresOpen formats a volume and loads its counters without starting a
// session, so no background flush or refresh interleaves with a scenario.
func volresOpen(t *testing.T, e volresEngine, capacity uint64, trashDays int) (Meta, *baseMeta) {
	t.Helper()
	m, err := e.open(t)
	if err != nil {
		t.Fatalf("create %s meta: %s", e.name, err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if e.reset {
		if err := m.Reset(); err != nil {
			t.Fatalf("reset %s: %s", e.name, err)
		}
	}
	format := testFormat()
	format.Capacity = capacity
	format.Inodes = 1 << 20
	format.TrashDays = trashDays
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init %s: %s", e.name, err)
	}
	b := m.getBase()
	b.refreshUsage()
	return m, b
}

func volresCtx(reserved bool) Context {
	ctx := Background()
	if reserved {
		ctx = withVolumeReservation(ctx, newVolumeReservation())
	}
	return ctx
}

func volresRelease(b *baseMeta, ctxs ...Context) {
	for _, ctx := range ctxs {
		if r := volumeReservationFrom(ctx); r != nil {
			b.releaseVolumeReservation(r)
		}
	}
}

func volresClaimed(b *baseMeta) int64 {
	space, _ := b.volumeClaimed()
	return space
}

func volresPending(b *baseMeta) (int64, int64) {
	b.volMu.Lock()
	defer b.volMu.Unlock()
	return b.pendingSpace, b.pendingInodes
}

func volresSetCapacity(b *baseMeta, capacity uint64) {
	f := *b.getFormat()
	f.Capacity = capacity
	b.setFormat(&f)
}

func volresNode(t *testing.T, m Meta, parent Ino, name string, typ uint8) Ino {
	t.Helper()
	var ino Ino
	if st := m.Mknod(Background(), parent, name, typ, 0755, 022, 0, "", &ino, nil); st != 0 {
		t.Fatalf("mknod %q: %s", name, st)
	}
	return ino
}

func volresGrow(t *testing.T, m Meta, ino Ino, size uint64) {
	t.Helper()
	if st := m.Fallocate(Background(), ino, 0, 0, size, nil); st != 0 {
		t.Fatalf("fallocate %d to %d: %s", ino, size, st)
	}
}

func volresCountTrips(t *testing.T) *atomic.Int32 {
	var trips atomic.Int32
	old := volumeQuotaHook
	volumeQuotaHook = func(ctx Context) {
		trips.Add(1)
		if old != nil {
			old(ctx)
		}
	}
	t.Cleanup(func() { volumeQuotaHook = old })
	return &trips
}

// volresHook pauses base operations at engine boundaries. It forwards every
// call to the real engine; it never substitutes metadata work.
type volresHook struct {
	engine
	afterFallocate   func(ino Ino)
	beforeCloneEntry func(top bool)
}

func (e *volresHook) doFallocate(ctx Context, inode Ino, mode uint8, off uint64, size uint64, delta *dirStat, attr *Attr) syscall.Errno {
	st := e.engine.doFallocate(ctx, inode, mode, off, size, delta, attr)
	if st == 0 && e.afterFallocate != nil {
		e.afterFallocate(inode)
	}
	return st
}

func (e *volresHook) doCloneEntry(ctx Context, srcIno Ino, parent Ino, name string, ino Ino, attr *Attr, cmode uint8, cumask uint16, top bool) syscall.Errno {
	if e.beforeCloneEntry != nil {
		e.beforeCloneEntry(top)
	}
	return e.engine.doCloneEntry(ctx, srcIno, parent, name, ino, attr, cmode, cumask, top)
}

func volresInstall(t *testing.T, b *baseMeta, h *volresHook) {
	t.Helper()
	old := b.en
	h.engine = old
	b.en = h
	t.Cleanup(func() { b.en = old })
}

func volresClone(m Meta, ctx Context, src Ino, name string) syscall.Errno {
	var count, total uint64
	return m.Clone(ctx, RootInode, src, RootInode, name, CLONE_MODE_PRESERVE_ATTR, 0, 1, &count, &total)
}

func volresArms(t *testing.T, run func(t *testing.T, e volresEngine, reserved bool)) {
	for _, e := range volresEngines {
		for _, reserved := range []bool{false, true} {
			arm := "control"
			if reserved {
				arm = "reserved"
			}
			t.Run(e.name+"/"+arm, func(t *testing.T) { run(t, e, reserved) })
		}
	}
}

const volresCapacity = 1 << 20

// A write that committed but is not yet counted must stay visible to the next
// check. Upstream counts it only after the transaction returns.
func TestVolumeReservationCoversCommittedUncountedWrite(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 0)
		a := volresNode(t, m, RootInode, "a", TypeFile)
		other := volresNode(t, m, RootInode, "b", TypeFile)
		free := uint64(volresCapacity - volresClaimed(b))
		half := (free/2 + 4096) &^ 4095 // each fits alone, both do not

		committed, resume := make(chan struct{}), make(chan struct{})
		volresInstall(t, b, &volresHook{afterFallocate: func(ino Ino) {
			if ino == a {
				close(committed)
				<-resume
			}
		}})
		ctxA, ctxB := volresCtx(reserved), volresCtx(reserved)
		done := make(chan syscall.Errno, 1)
		go func() { done <- m.Fallocate(ctxA, a, 0, 0, half, nil) }()
		<-committed
		stB := m.Fallocate(ctxB, other, 0, 0, half, nil)
		close(resume)
		if st := <-done; st != 0 {
			t.Fatalf("first fallocate: %s", st)
		}
		volresRelease(b, ctxA, ctxB)

		used := volresClaimed(b)
		if !reserved {
			if stB != 0 || used <= volresCapacity {
				t.Fatalf("control arm did not reach the window: second=%s used=%d capacity=%d", stB, used, volresCapacity)
			}
			return
		}
		if stB != syscall.ENOSPC {
			t.Fatalf("second fallocate = %s, want ENOSPC", stB)
		}
		if used > volresCapacity {
			t.Fatalf("used %d exceeds capacity %d", used, volresCapacity)
		}
		if s, i := volresPending(b); s != 0 || i != 0 {
			t.Fatalf("pending after release = %d/%d, want 0/0", s, i)
		}
	})
}

// A clone paused between its preflight and its first entry holds its whole
// summary against the ceiling; upstream holds nothing in that interval.
func TestVolumeReservationHoldsClonePreflight(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 0)
		src := volresNode(t, m, RootInode, "src", TypeDirectory)
		volresGrow(t, m, volresNode(t, m, src, "f", TypeFile), 4*4096)
		other := volresNode(t, m, RootInode, "other", TypeFile)
		summary := uint64(4096 + 4*4096)
		free := uint64(volresCapacity - volresClaimed(b))
		write := free - summary + 4096 // fits alone, not together with the clone

		preflighted, resume := make(chan struct{}), make(chan struct{})
		var paused atomic.Bool
		volresInstall(t, b, &volresHook{beforeCloneEntry: func(top bool) {
			if top && paused.CompareAndSwap(false, true) {
				close(preflighted)
				<-resume
			}
		}})
		ctxClone, ctxWrite := volresCtx(reserved), volresCtx(reserved)
		done := make(chan syscall.Errno, 1)
		go func() { done <- volresClone(m, ctxClone, src, "dst") }()
		<-preflighted
		stWrite := m.Fallocate(ctxWrite, other, 0, 0, write, nil)
		close(resume)
		if st := <-done; st != 0 {
			t.Fatalf("clone: %s", st)
		}
		volresRelease(b, ctxClone, ctxWrite)

		used := volresClaimed(b)
		if !reserved {
			if stWrite != 0 || used <= volresCapacity {
				t.Fatalf("control arm did not reach the window: write=%s used=%d", stWrite, used)
			}
			return
		}
		if stWrite != syscall.ENOSPC {
			t.Fatalf("write during clone = %s, want ENOSPC", stWrite)
		}
		if used > volresCapacity {
			t.Fatalf("used %d exceeds capacity %d", used, volresCapacity)
		}
	})
}

// Two clones near the ceiling: the second preflight sees the first claim.
func TestVolumeReservationSerializesNearLimitClones(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 0)
		src := volresNode(t, m, RootInode, "src", TypeDirectory)
		file := volresNode(t, m, src, "f", TypeFile)
		free := uint64(volresCapacity - volresClaimed(b))
		size := (free / 2) &^ 4095 // one clone fits, two do not
		volresGrow(t, m, file, size)
		if left := uint64(volresCapacity - volresClaimed(b)); 4096+size > left || 2*(4096+size) <= left {
			t.Fatalf("setup: clone of %d with %d free must fit once and not twice", 4096+size, left)
		}

		preflighted, resume := make(chan struct{}), make(chan struct{})
		var paused atomic.Bool
		volresInstall(t, b, &volresHook{beforeCloneEntry: func(top bool) {
			if top && paused.CompareAndSwap(false, true) {
				close(preflighted)
				<-resume
			}
		}})
		ctx1, ctx2 := volresCtx(reserved), volresCtx(reserved)
		done := make(chan syscall.Errno, 1)
		go func() { done <- volresClone(m, ctx1, src, "one") }()
		<-preflighted
		st2 := volresClone(m, ctx2, src, "two")
		close(resume)
		if st := <-done; st != 0 {
			t.Fatalf("first clone: %s", st)
		}
		volresRelease(b, ctx1, ctx2)

		used := volresClaimed(b)
		if !reserved {
			if st2 != 0 || used <= volresCapacity {
				t.Fatalf("control arm did not reach the window: second=%s used=%d", st2, used)
			}
			return
		}
		if st2 != syscall.ENOSPC {
			t.Fatalf("second clone = %s, want ENOSPC", st2)
		}
		if used > volresCapacity {
			t.Fatalf("used %d exceeds capacity %d", used, volresCapacity)
		}
	})
}

// A write committed but not yet counted must be seen by a clone preflight.
func TestVolumeReservationClonePreflightSeesUncountedWrite(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 0)
		src := volresNode(t, m, RootInode, "src", TypeDirectory)
		volresGrow(t, m, volresNode(t, m, src, "f", TypeFile), 4*4096)
		other := volresNode(t, m, RootInode, "other", TypeFile)
		summary := uint64(4096 + 4*4096)
		free := uint64(volresCapacity - volresClaimed(b))
		write := free - summary + 4096

		committed, resume := make(chan struct{}), make(chan struct{})
		volresInstall(t, b, &volresHook{afterFallocate: func(ino Ino) {
			if ino == other {
				close(committed)
				<-resume
			}
		}})
		ctxWrite, ctxClone := volresCtx(reserved), volresCtx(reserved)
		done := make(chan syscall.Errno, 1)
		go func() { done <- m.Fallocate(ctxWrite, other, 0, 0, write, nil) }()
		<-committed
		stClone := volresClone(m, ctxClone, src, "dst")
		close(resume)
		if st := <-done; st != 0 {
			t.Fatalf("write: %s", st)
		}
		volresRelease(b, ctxWrite, ctxClone)

		used := volresClaimed(b)
		if !reserved {
			if stClone != 0 || used <= volresCapacity {
				t.Fatalf("control arm did not reach the window: clone=%s used=%d", stClone, used)
			}
			return
		}
		if stClone != syscall.ENOSPC {
			t.Fatalf("clone after uncounted write = %s, want ENOSPC", stClone)
		}
		if used > volresCapacity {
			t.Fatalf("used %d exceeds capacity %d", used, volresCapacity)
		}
	})
}

// The source grows after the preflight. Every clone transaction is charged
// before it commits, so the growth the preflight did not see is refused
// without the volume quota hook (no admission retry of a partial clone), the
// detached copy is removed, and nothing stays claimed.
func TestVolumeReservationChargesCloneTransactionsBeforeCommit(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 0)
		src := volresNode(t, m, RootInode, "src", TypeDirectory)
		volresGrow(t, m, volresNode(t, m, src, "f1", TypeFile), 2*4096)
		f2 := volresNode(t, m, src, "f2", TypeFile)
		volresGrow(t, m, f2, 2*4096)
		summary := uint64(4096 + 4*4096)
		before := volresClaimed(b)
		growth := uint64(volresCapacity-before) - summary

		var grown atomic.Bool
		volresInstall(t, b, &volresHook{beforeCloneEntry: func(top bool) {
			if top && grown.CompareAndSwap(false, true) {
				volresGrow(t, m, f2, 2*4096+growth)
			}
		}})
		trips := volresCountTrips(t)
		ctx := volresCtx(reserved)
		st := volresClone(m, ctx, src, "dst")
		volresRelease(b, ctx)

		used := volresClaimed(b)
		if !reserved {
			if st != 0 || used <= volresCapacity {
				t.Fatalf("control arm did not reach the window: clone=%s used=%d", st, used)
			}
			return
		}
		if st != syscall.ENOSPC {
			t.Fatalf("clone of a grown source = %s, want ENOSPC", st)
		}
		if got := trips.Load(); got != 0 {
			t.Fatalf("a refused clone charge tripped the volume quota hook %d times; that would retry a partial clone", got)
		}
		var ino Ino
		var attr Attr
		if st := m.Lookup(Background(), RootInode, "dst", &ino, &attr, false); st != syscall.ENOENT {
			t.Fatalf("destination after refused clone = %s, want ENOENT", st)
		}
		if want := before + int64(growth); used != want {
			t.Fatalf("used after refused clone = %d, want %d (source growth only)", used, want)
		}
		if s, i := volresPending(b); s != 0 || i != 0 {
			t.Fatalf("pending after release = %d/%d", s, i)
		}
		if b.unreservedSpace != 0 || b.unreservedInodes != 0 {
			t.Fatalf("unreserved growth %d/%d", b.unreservedSpace, b.unreservedInodes)
		}
	})
}

// A clone of a quiesced tree converts exactly its preflight: every entry type,
// directory, empty and sparse file, symlink, through both cloneEntry and
// BatchClone, leaves no leftover claim and no unreserved growth.
func TestVolumeReservationCloneSummaryEqualsCharges(t *testing.T) {
	for _, e := range volresEngines {
		t.Run(e.name, func(t *testing.T) {
			m, b := volresOpen(t, e, volresCapacity, 0)
			src := volresNode(t, m, RootInode, "src", TypeDirectory)
			volresNode(t, m, src, "empty", TypeFile)
			volresGrow(t, m, volresNode(t, m, src, "sparse", TypeFile), 3*4096+1)
			sub := volresNode(t, m, src, "sub", TypeDirectory)
			volresGrow(t, m, volresNode(t, m, sub, "inner", TypeFile), 4096)
			var link Ino
			if st := m.Symlink(Background(), src, "link", "sparse", &link, nil); st != 0 {
				t.Fatalf("symlink: %s", st)
			}
			var sum Summary
			if st := m.GetSummary(Background(), src, &sum, true, true); st != 0 {
				t.Fatalf("summary: %s", st)
			}
			before := volresClaimed(b)
			ctx := volresCtx(true)
			if st := volresClone(m, ctx, src, "dst"); st != 0 {
				t.Fatalf("clone: %s", st)
			}
			r := volumeReservationFrom(ctx)
			b.volMu.Lock()
			leftSpace, leftInodes, charges := r.space, r.inodes, len(r.charges)
			b.volMu.Unlock()
			if leftSpace != 0 || leftInodes != 0 || charges != 0 {
				t.Fatalf("leftover claim %d/%d with %d charges, want none", leftSpace, leftInodes, charges)
			}
			volresRelease(b, ctx)
			if got := volresClaimed(b) - before; got != int64(sum.Size) {
				t.Fatalf("clone counted %d bytes, summary %d", got, sum.Size)
			}
			if b.unreservedSpace != 0 || b.unreservedInodes != 0 {
				t.Fatalf("unreserved growth %d/%d", b.unreservedSpace, b.unreservedInodes)
			}
		})
	}
}

// A clone canceled after its preflight claims nothing once it returns. The
// SQL clone transaction rechecks cancellation before commit; the other engines
// may commit the detached root first, which then stays counted, never lost.
func TestVolumeReservationCanceledCloneReleasesClaim(t *testing.T) {
	for _, e := range volresEngines {
		t.Run(e.name, func(t *testing.T) {
			m, b := volresOpen(t, e, volresCapacity, 0)
			src := volresNode(t, m, RootInode, "src", TypeDirectory)
			volresGrow(t, m, volresNode(t, m, src, "f", TypeFile), 4096)
			ctx := volresCtx(true)
			volresInstall(t, b, &volresHook{beforeCloneEntry: func(top bool) {
				if top {
					ctx.Cancel()
				}
			}})
			before := volresClaimed(b)
			st := volresClone(m, ctx, src, "dst")
			if st == 0 || (e.name == "sqlite3" && st != syscall.EINTR) {
				t.Fatalf("canceled clone = %s", st)
			}
			volresRelease(b, ctx)
			if s, i := volresPending(b); s != 0 || i != 0 {
				t.Fatalf("pending after canceled clone = %d/%d", s, i)
			}
			used := volresClaimed(b)
			if used != before && used != before+4096 {
				t.Fatalf("used after canceled clone = %d, want %d or the committed root %d", used, before, before+4096)
			}
			if e.name == "sqlite3" && used != before {
				t.Fatalf("SQL clone committed after cancellation: used %d, want %d", used, before)
			}
		})
	}
}

// Ledger rules independent of any engine path: a retried check replaces its
// own claim (no self refusal), a retried transaction replaces its own charge,
// separate calls add up, and release returns everything.
func TestVolumeReservationLedgerRules(t *testing.T) {
	_, b := volresOpen(t, volresSQLiteEngine, volresCapacity, 0)
	free := int64(volresCapacity) - volresClaimed(b)

	one := newVolumeReservation()
	if !b.reserveVolume(one, free, 1) {
		t.Fatal("a claim of all free space was refused")
	}
	if !b.reserveVolume(one, free, 1) {
		t.Fatal("a retried attempt was refused by its own earlier claim")
	}
	if s, _ := volresPending(b); s != free {
		t.Fatalf("pending after a retried claim = %d, want %d", s, free)
	}
	two := newVolumeReservation()
	if b.reserveVolume(two, 4096, 0) {
		t.Fatal("a second call was admitted into space the first one claimed")
	}
	b.releaseVolumeReservation(one)
	b.releaseVolumeReservation(two)
	if s, i := volresPending(b); s != 0 || i != 0 {
		t.Fatalf("pending after release = %d/%d", s, i)
	}

	ctx := withVolumeReservation(Background(), newVolumeReservation())
	r := volumeReservationFrom(ctx)
	if !b.reserveVolume(r, 8*4096, 2) {
		t.Fatal("preflight refused")
	}
	for attempt := 0; attempt < 3; attempt++ {
		if st := b.chargeVolume(ctx, Ino(1000), 3*4096, 1); st != 0 {
			t.Fatalf("charge attempt %d: %s", attempt, st)
		}
	}
	if s, i := volresPending(b); s != 8*4096 || i != 2 {
		t.Fatalf("pending after retried charges = %d/%d, want the preflight 32768/2", s, i)
	}
	if st := b.chargeVolume(ctx, Ino(2000), free, 0); st != syscall.ENOSPC {
		t.Fatalf("charge beyond the ceiling = %s, want ENOSPC", st)
	}
	newBefore := atomic.LoadInt64(&b.newSpace)
	b.commitVolume(ctx, []Ino{1000}, 3*4096, 1)
	if got := atomic.LoadInt64(&b.newSpace) - newBefore; got != 3*4096 {
		t.Fatalf("converted %d, want %d", got, 3*4096)
	}
	if s, i := volresPending(b); s != 5*4096 || i != 1 {
		t.Fatalf("pending after conversion = %d/%d, want 20480/1", s, i)
	}
	b.releaseVolumeReservation(r)
	if s, i := volresPending(b); s != 0 || i != 0 {
		t.Fatalf("pending after release = %d/%d", s, i)
	}
	if b.unreservedSpace != 0 || b.unreservedInodes != 0 {
		t.Fatalf("unreserved growth %d/%d", b.unreservedSpace, b.unreservedInodes)
	}
}

// The flush moves its delta from newSpace to usedSpace in two steps. A
// lock-free reader between them undercounts by the delta; the reservation
// check is excluded from that interval by volMu. Redis has no such transfer:
// its committed deltas are already in usedSpace, so its flush moves nothing.
func TestVolumeReservationFlushTransferIsAtomic(t *testing.T) {
	for _, e := range volresEngines {
		t.Run(e.name, func(t *testing.T) {
			m, b := volresOpen(t, e, volresCapacity, 0)
			volresNode(t, m, RootInode, "f", TypeFile)
			total := atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace)
			var invoked, excluded bool
			var lockFree int64
			hook := func() {
				invoked = true
				lockFree = atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace)
				if b.volMu.TryLock() {
					b.volMu.Unlock()
				} else {
					excluded = true
				}
			}
			volumeTransferTestHook.Store(&hook)
			t.Cleanup(func() { volumeTransferTestHook.Store(nil) })
			b.doFlushStats()
			if e.name == "redis" {
				if invoked || atomic.LoadInt64(&b.newSpace) != 0 {
					t.Fatalf("redis flush transferred something (invoked=%t newSpace=%d)", invoked, atomic.LoadInt64(&b.newSpace))
				}
				if got := volresClaimed(b); got != total {
					t.Fatalf("after flush %d, want %d", got, total)
				}
				return
			}
			if !invoked {
				t.Fatal("flush did not transfer anything")
			}
			if lockFree != total-4096 {
				t.Fatalf("lock-free reader saw %d mid-transfer, want the %d undercount that makes the lock necessary", lockFree, total-4096)
			}
			if !excluded {
				t.Fatal("the flush transfer does not hold volMu; a reservation check can see the undercount")
			}
			if got := volresClaimed(b); got != total {
				t.Fatalf("after flush %d, want %d", got, total)
			}
		})
	}
}

// refresh used to read the persisted counter and store it unconditionally. A
// delta that reaches usedSpace after the read and before the store is lost: on
// SQL and KV through a flush committing in between, on Redis through the
// mutation's own in-memory add (updateStats), which follows its remote commit.
func TestVolumeReservationRefreshDoesNotOverwriteAFlush(t *testing.T) {
	for _, e := range volresEngines {
		t.Run(e.name+"/control", func(t *testing.T) {
			m, b := volresOpen(t, e, volresCapacity, 0)
			stale, err := b.en.getCounter(usedSpace)
			if err != nil {
				t.Fatal(err)
			}
			volresNode(t, m, RootInode, "f", TypeFile)
			total := atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace)
			b.doFlushStats()
			atomic.StoreInt64(&b.usedSpace, stale) // the old unguarded store
			if got := volresClaimed(b); got != total-4096 {
				t.Fatalf("old interleaving counted %d, want the %d undercount", got, total-4096)
			}
		})
		t.Run(e.name+"/guarded", func(t *testing.T) {
			m, b := volresOpen(t, e, volresCapacity, 0)
			volresNode(t, m, RootInode, "f", TypeFile)
			total := atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace)
			flushed := make(chan struct{})
			var excluded, early bool
			hook := func() {
				if b.fsStatsLock.TryLock() {
					b.fsStatsLock.Unlock()
				} else {
					excluded = true
				}
				go func() {
					b.doFlushStats()
					close(flushed)
				}()
				select {
				case <-flushed:
					early = true
				case <-time.After(20 * time.Millisecond):
				}
			}
			refreshUsageTestHook.Store(&hook)
			b.refreshUsage()
			refreshUsageTestHook.Store(nil)
			<-flushed
			if !excluded || early {
				t.Fatalf("a flush can commit between refresh's read and store (excluded=%t early=%t)", excluded, early)
			}
			if got := volresClaimed(b); got != total {
				t.Fatalf("after refresh and flush %d, want %d", got, total)
			}
			persisted, err := b.en.getCounter(usedSpace)
			if err != nil {
				t.Fatal(err)
			}
			if got := atomic.LoadInt64(&b.usedSpace); got != persisted {
				t.Fatalf("usedSpace %d, persisted %d", got, persisted)
			}
		})
	}
}

// A remote Redis commit can precede its local accounting. Replacing the
// local baseline in that interval counts the delta twice, including deletion.
func TestVolumeReservationRedisRefreshDoesNotDoubleCountRemoteCommit(t *testing.T) {
	for _, delta := range []int64{4096, -4096} {
		for _, guarded := range []bool{false, true} {
			t.Run(fmt.Sprintf("delta=%d/guarded=%t", delta, guarded), func(t *testing.T) {
				m, b := volresOpen(t, volresRedisEngine, volresCapacity, 0)
				volresNode(t, m, RootInode, "existing", TypeFile)
				before := volresClaimed(b)
				b.enableSingleWriterCounters(guarded)
				// Same remote-before-local ordering as Redis metadata commits.
				if _, err := b.en.incrCounter(usedSpace, delta); err != nil {
					t.Fatal(err)
				}
				b.refreshUsage()
				b.en.updateStats(delta, 0)
				want := before + 2*delta
				if guarded {
					want = before + delta
				}
				if got := volresClaimed(b); got != want {
					t.Fatalf("usage=%d want=%d", got, want)
				}
				b.refreshUsage()
				if got := volresClaimed(b); got != before+delta {
					t.Fatalf("settled usage=%d want=%d", got, before+delta)
				}
			})
		}
	}
}

// Growth committed after an old remote read cannot be lost in strict mode.
func TestVolumeReservationRedisKeepsLocalCommit(t *testing.T) {
	m, b := volresOpen(t, volresRedisEngine, volresCapacity, 0)
	b.enableSingleWriterCounters(true)
	before := volresClaimed(b)
	ctx := volresCtx(true)
	var ino Ino
	if st := m.Mknod(ctx, RootInode, "late", TypeFile, 0644, 022, 0, "", &ino, nil); st != 0 {
		t.Fatal(st)
	}
	volresRelease(b, ctx)
	b.refreshUsage()
	if got := volresClaimed(b); got != before+4096 {
		t.Fatalf("usage=%d want=%d", got, before+4096)
	}
}

// The first deletion of an hour creates the trash bucket, 4 KiB and one inode.
// At a full ceiling the reserved call is refused before it changes anything;
// the trash is never bypassed; once the bucket exists deletion needs no room.
func TestVolumeReservationClaimsTheTrashBucket(t *testing.T) {
	volresArms(t, func(t *testing.T, e volresEngine, reserved bool) {
		m, b := volresOpen(t, e, volresCapacity, 1)
		victim := volresNode(t, m, RootInode, "victim", TypeFile)
		volresNode(t, m, RootInode, "second", TypeFile)
		volresNode(t, m, RootInode, "renamed", TypeFile)
		bucket := TrashBucketName(time.Now())
		full := uint64(volresClaimed(b))
		volresSetCapacity(b, full)
		trips := volresCountTrips(t)

		ctx := volresCtx(reserved)
		st := m.Unlink(ctx, RootInode, "victim")
		volresRelease(b, ctx)
		if !reserved {
			if st != 0 || volresClaimed(b) <= int64(full) {
				t.Fatalf("control arm did not grow past the ceiling: unlink=%s used=%d", st, volresClaimed(b))
			}
			return
		}
		if st != syscall.ENOSPC || trips.Load() != 1 {
			t.Fatalf("unlink at a full ceiling = %s with %d trips, want ENOSPC with one", st, trips.Load())
		}
		var ino Ino
		var attr Attr
		if st := m.Lookup(Background(), RootInode, "victim", &ino, &attr, false); st != 0 {
			t.Fatalf("refused unlink removed the file: %s", st)
		}
		ctx = volresCtx(true)
		var movedIno Ino
		var movedAttr Attr
		st = m.Rename(ctx, RootInode, "renamed", RootInode, "moved", 0, &movedIno, &movedAttr)
		volresRelease(b, ctx)
		if st != syscall.ENOSPC {
			t.Fatalf("rename at a full ceiling = %s, want ENOSPC (upstream resolves the bucket for every rename)", st)
		}

		volresSetCapacity(b, full+4096)
		ctx = volresCtx(true)
		if st := m.Unlink(ctx, RootInode, "victim"); st != 0 {
			t.Fatalf("unlink with room for the bucket: %s", st)
		}
		volresRelease(b, ctx)
		if TrashBucketName(time.Now()) != bucket {
			t.Skip("the hour changed during the test")
		}
		var bucketIno Ino
		if st := m.Lookup(Background(), TrashInode, bucket, &bucketIno, &attr, false); st != 0 {
			t.Fatalf("trash bucket: %s", st)
		}
		if st := m.Lookup(Background(), bucketIno, TrashEntryName(RootInode, victim, "victim"), &ino, &attr, false); st != 0 {
			t.Fatalf("the deleted file is not in the trash: %s", st)
		}

		volresSetCapacity(b, uint64(volresClaimed(b)))
		ctx = volresCtx(true)
		if st := m.Unlink(ctx, RootInode, "second"); st != 0 {
			t.Fatalf("unlink into an existing bucket at a full ceiling: %s", st)
		}
		volresRelease(b, ctx)
		if used := volresClaimed(b); used > int64(b.getFormat().Capacity) {
			t.Fatalf("used %d exceeds capacity %d", used, b.getFormat().Capacity)
		}
		if s, i := volresPending(b); s != 0 || i != 0 {
			t.Fatalf("pending %d/%d", s, i)
		}
	})
}

func TestVolumeReservationRedisFailedDetachedCleanupKeepsUsage(t *testing.T) {
	m, b := volresOpen(t, volresRedisEngine, volresCapacity, 0)
	r := m.(*redisMeta)
	ino := volresNode(t, m, RootInode, "detached", TypeDirectory)
	if err := r.rdb.HDel(Background(), r.entryKey(RootInode), "detached").Err(); err != nil {
		t.Fatal(err)
	}
	b.enableSingleWriterCounters(true)
	before := volresClaimed(b)
	// A concrete transaction refusal after successful lookup/empty enumeration.
	r.conf.ReadOnly = true
	if st := r.doCleanupDetachedNode(Background(), ino); st != syscall.EROFS {
		t.Fatalf("cleanup=%s want EROFS", st)
	}
	r.conf.ReadOnly = false
	if got := volresClaimed(b); got != before {
		t.Fatalf("failed cleanup freed usage: %d want %d", got, before)
	}
	if st := r.doCleanupDetachedNode(Background(), ino); st != 0 {
		t.Fatal(st)
	}
	if got := volresClaimed(b); got != before-4096 {
		t.Fatalf("successful cleanup usage=%d want=%d", got, before-4096)
	}
	if st := r.doCleanupDetachedNode(Background(), ino); st != 0 {
		t.Fatal(st)
	}
	if got := volresClaimed(b); got != before-4096 {
		t.Fatalf("replayed cleanup usage=%d want=%d", got, before-4096)
	}
}
