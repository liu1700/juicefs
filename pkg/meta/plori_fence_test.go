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

package meta

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The acceptance close-out for PLO-362 and for PLO-323's "stale holders fail
// writes loudly".
//
// Both features shipped in fork #35 with no test of their own: before this
// file, `grep -rn 'PloriPurgeAllSessions\|PloriFenceWrites' --include='*_test.go'`
// matched nothing in the tree, so PLO-362's closing note ("Unit-tested") was
// not true of the fork as merged.

// openRestoredMeta opens a second client on a metadata database another writer
// already created. That is exactly the shape a restored replica has: the file
// carries the previous generation's rows, and this process is about to become
// its only legitimate writer.
//
// The Load is not optional. PloriPurgeAllSessions reaches doCleanStaleSession,
// which calls genLog, which dereferences the cached Format (sql.go:1131) — on a
// client that never loaded it that is a nil dereference, not an error. The
// production caller loads the Format inside FS.Open before the supervisor asks
// for the sweep (cmd/plori_mount.go:277, supervisor.go:155 then :177), so the
// order is right there; TestPloriPurgeAllSessionsNeedsALoadedFormat below pins
// the precondition so nobody reorders it by accident.
func openRestoredMeta(t *testing.T, dbPath string) Meta {
	t.Helper()
	m, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("open sqlite meta %s: %s", dbPath, err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := m.Load(true); err != nil {
		t.Fatalf("load format from %s: %s", dbPath, err)
	}
	return m
}

// TestPloriPurgeAllSessionsFreesADeadWritersFlockImmediately is the test PLO-362
// specified verbatim: "restore a DB with a synthetic stale session + flock,
// mount, assert the lock is free immediately".
//
// The point is the word "immediately". JuiceFS reaps a session only once its
// Expire has passed, and Expire is `now + 5*heartbeat` (base.go expireTime); at
// the Plori profile's --heartbeat 300 that is 25 minutes. Without the total
// sweep the live writer would sit behind a dead writer's lock for that long,
// which is why the sweep is total rather than age-based.
func TestPloriPurgeAllSessionsFreesADeadWritersFlockImmediately(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restored.db")
	ctx := Background()

	// ---- generation N: the writer that dies holding a lock ----
	dead, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := dead.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := dead.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := dead.NewSession(true); err != nil {
		t.Fatalf("open the dying writer's session: %s", err)
	}

	var inode Ino
	var attr Attr
	if st := dead.Create(ctx, RootInode, "held", 0644, 022, 0, &inode, &attr); st != 0 {
		t.Fatalf("create held: %s", st)
	}
	const deadOwner = uint64(0xDEAD000000000001)
	if st := dead.Flock(ctx, inode, deadOwner, syscall.F_WRLCK, false); st != 0 {
		t.Fatalf("dead writer flock: %s", st)
	}
	// It dies here: no CloseSession, so the session2 row, its far-future
	// Expire and its flock row all survive into what the next writer restores.

	// ---- generation N+1: the lease holder that restores that image ----
	live := openRestoredMeta(t, dbPath)

	sessions, err := live.ListSessions()
	if err != nil {
		t.Fatalf("list sessions on the restored replica: %s", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("restored replica carries %d sessions, want the 1 the dead writer left", len(sessions))
	}

	const liveOwner = uint64(0x0000000000000001)
	if st := live.Flock(ctx, inode, liveOwner, syscall.F_WRLCK, false); st != syscall.EAGAIN {
		t.Fatalf("before the sweep the dead writer's lock must still block: got %v, want EAGAIN", st)
	}

	n, err := PloriPurgeAllSessions(live)
	if err != nil {
		t.Fatalf("purge sessions: %s", err)
	}
	if n != 1 {
		t.Fatalf("purged %d sessions, want 1", n)
	}

	if _, flocks, err := live.ListLocks(ctx, inode); err != nil || len(flocks) != 0 {
		t.Fatalf("after the sweep flocks = %v (err %v), want none", flocks, err)
	}
	if st := live.Flock(ctx, inode, liveOwner, syscall.F_WRLCK, false); st != 0 {
		t.Fatalf("the live writer must take the lock immediately after the sweep: %s", st)
	}
	if sessions, err := live.ListSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("after the sweep sessions = %v (err %v), want none", sessions, err)
	}
}

// TestPloriPurgeAllSessionsSweepsEverySessionNotJustTheExpiredOnes pins the
// difference from upstream CleanStaleSessions, which is the whole reason
// PLO-362 exists: three live-looking sessions, none of them expired, all gone.
func TestPloriPurgeAllSessionsSweepsEverySessionNotJustTheExpiredOnes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "many.db")

	first, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := first.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := first.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := first.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}
	for i := 0; i < 2; i++ {
		// openRestoredMeta loads the Format; NewSession dereferences the ACL
		// cache it populates (sql.go:777).
		other := openRestoredMeta(t, dbPath)
		if err := other.NewSession(true); err != nil {
			t.Fatalf("extra session %d: %s", i, err)
		}
	}

	live := openRestoredMeta(t, dbPath)
	before, err := live.ListSessions()
	if err != nil {
		t.Fatalf("list sessions: %s", err)
	}
	if len(before) != 3 {
		t.Fatalf("set up %d sessions, want 3", len(before))
	}
	// None of them is stale by upstream's rule: Expire is heartbeat*5 ahead.
	for _, s := range before {
		if !s.Expire.After(time.Now()) {
			t.Fatalf("session %d already expired (%s); the test would not prove anything", s.Sid, s.Expire)
		}
	}

	n, err := PloriPurgeAllSessions(live)
	if err != nil {
		t.Fatalf("purge: %s", err)
	}
	if n != 3 {
		t.Fatalf("purged %d, want 3", n)
	}
	if after, err := live.ListSessions(); err != nil || len(after) != 0 {
		t.Fatalf("after purge %d sessions remain (err %v), want 0", len(after), err)
	}
}

// notACleaner satisfies Meta without the engine-level hook the sweep needs.
// A metadata engine the sweep cannot drive has to be a refusal, because the
// caller's contract is to prove the sweep happened before it mounts.
type notACleaner struct{ Meta }

func TestPloriPurgeAllSessionsRefusesAnEngineItCannotSweep(t *testing.T) {
	n, err := PloriPurgeAllSessions(notACleaner{})
	if err == nil {
		t.Fatal("an engine without doCleanStaleSession must be refused, not silently skipped")
	}
	if n != 0 {
		t.Fatalf("refusal reported %d swept sessions, want 0", n)
	}
}

// TestPloriPurgeAllSessionsNeedsALoadedFormat pins the one precondition the
// sweep has and does not state.
//
// The sweep is a fail-closed gate: the supervisor turns its error into exit 67
// and refuses to mount (supervisor.go:177-181). But on a client whose Format
// was never loaded it does not return an error at all — doCleanStaleSession
// reaches genLog, which dereferences the nil cached Format (sql.go:1131) and
// panics. A panic exits 2, which is not in the plugin's exit-code table
// (64-70), so the refusal the design intends would reach fuse-csi-node as an
// unclassified crash.
//
// The production order is correct today (FS.Open loads the Format before the
// supervisor calls PurgeSessions), so this is a latent trap rather than a live
// defect. The test states the requirement so a future caller cannot violate it
// silently, and documents the one-line fix: PloriPurgeAllSessions should refuse
// a client with no Format instead of relying on its caller.
func TestPloriPurgeAllSessionsNeedsALoadedFormat(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "unloaded.db")
	seed, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := seed.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := seed.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := seed.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}

	unloaded, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("second client: %s", err)
	}
	t.Cleanup(func() { _ = unloaded.Shutdown() })

	var panicked bool
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		_, _ = PloriPurgeAllSessions(unloaded)
	}()
	if !panicked {
		t.Log("PloriPurgeAllSessions no longer needs a loaded Format — the latent trap is fixed; " +
			"drop this test or invert it to assert the typed refusal")
		return
	}
	// Same client, Format loaded: the sweep works. That is the contract the
	// production caller already satisfies.
	if _, err := unloaded.Load(true); err != nil {
		t.Fatalf("load format: %s", err)
	}
	if n, err := PloriPurgeAllSessions(unloaded); err != nil || n != 1 {
		t.Fatalf("sweep with a loaded format = (%d, %v), want (1, nil)", n, err)
	}
}

// --------------------------------------------------------- the write fence ---

// fenceForTest flips the process-wide fence and puts it back. The production
// fence is deliberately one-way — nothing un-fences a writer, because the
// authority it lost is an epoch that is never reissued (plori_fence.go:34-42) —
// so a test that trips it has to restore the package variable directly, which
// is legal only from inside package meta.
func fenceForTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { ploriFenced.Store(false) })
	PloriFenceWrites()
	if !PloriWritesFenced() {
		t.Fatal("PloriFenceWrites did not take effect")
	}
}

func fencedTestMeta(t *testing.T) (Meta, Ino) {
	t.Helper()
	m, err := newSQLMeta("sqlite3", filepath.Join(t.TempDir(), "fence.db"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "f", 0644, 022, 0, &inode, &attr); st != 0 {
		t.Fatalf("create f: %s", st)
	}
	if st := m.Mkdir(ctx, RootInode, "d", 0755, 022, 0, &inode2Discard, &attr); st != 0 {
		t.Fatalf("mkdir d: %s", st)
	}
	return m, inode
}

var inode2Discard Ino

// TestPloriFenceWritesMakesNamespaceMutationsEROFS is PLO-323's "stale holders
// fail writes loudly", at the layer that owns it. After the fence every
// operation that creates, removes, renames or re-links a name answers EROFS,
// and so does any attempt to newly open a file for writing.
func TestPloriFenceWritesMakesNamespaceMutationsEROFS(t *testing.T) {
	m, inode := fencedTestMeta(t)
	ctx := Background()
	var attr Attr
	var made Ino

	fenceForTest(t)

	cases := []struct {
		name string
		run  func() syscall.Errno
	}{
		{"Mknod", func() syscall.Errno {
			return m.Mknod(ctx, RootInode, "new", TypeFile, 0644, 022, 0, "", &made, &attr)
		}},
		{"Create", func() syscall.Errno {
			return m.Create(ctx, RootInode, "new2", 0644, 022, 0, &made, &attr)
		}},
		{"Mkdir", func() syscall.Errno {
			return m.Mkdir(ctx, RootInode, "newdir", 0755, 022, 0, &made, &attr)
		}},
		{"Link", func() syscall.Errno { return m.Link(ctx, inode, RootInode, "alias", &attr) }},
		{"Unlink", func() syscall.Errno { return m.Unlink(ctx, RootInode, "f") }},
		{"Rmdir", func() syscall.Errno { return m.Rmdir(ctx, RootInode, "d") }},
		{"Rename", func() syscall.Errno {
			return m.Rename(ctx, RootInode, "f", RootInode, "f2", 0, &made, &attr)
		}},
		{"SetXattr", func() syscall.Errno {
			return m.SetXattr(ctx, inode, "user.k", []byte("v"), 0)
		}},
		{"RemoveXattr", func() syscall.Errno { return m.RemoveXattr(ctx, inode, "user.k") }},
		{"Open O_WRONLY", func() syscall.Errno {
			return m.Open(ctx, inode, syscall.O_WRONLY, &attr)
		}},
	}
	for _, c := range cases {
		if st := c.run(); st != syscall.EROFS {
			t.Errorf("%s after the fence = %v, want EROFS", c.name, st)
		}
	}

	// Reads keep working: the write-stop margin exists so the writeback cache
	// can drain inside the lease it was written under.
	if st := m.Open(ctx, inode, syscall.O_RDONLY, &attr); st != 0 {
		t.Errorf("read-only open after the fence = %v, want success", st)
	}
	if st := m.GetAttr(ctx, inode, &attr); st != 0 {
		t.Errorf("GetAttr after the fence = %v, want success", st)
	}
}

// TestPloriFenceWritesStopsWritesThroughAnAlreadyOpenFile is PLO-323's "stale
// holders fail writes loudly" on the path that actually carries an Agent's
// bytes, and it is the assertion the acceptance audit refuted (F-2).
//
// The fence used to reuse only upstream's nine namespace call sites. Open with
// write flags was among them, so no NEW writable handle could be obtained — but
// Write, Truncate, SetAttr and Fallocate were not, and a handle opened before
// the fence kept committing slices into the metadata Litestream replicates. An
// Agent mid-`git clone` holds many such handles, and with no server-side fence
// on the object store (no STS, no per-prefix credentials — PLO-351) the
// writer's own cooperation is the only thing between a revoked epoch and a
// divergent filesystem.
//
// It is also the half of PLO-312 that was never superseded: "on revocation,
// invalidate handles" (acceptance A4). This gate is that preservation.
func TestPloriFenceWritesStopsWritesThroughAnAlreadyOpenFile(t *testing.T) {
	m, inode := fencedTestMeta(t)
	ctx := Background()
	var attr Attr

	// The handle is obtained BEFORE the fence, which is the realistic case.
	if st := m.Open(ctx, inode, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open before the fence: %s", st)
	}

	fenceForTest(t)

	cases := []struct {
		name string
		run  func() syscall.Errno
	}{
		{"Write", func() syscall.Errno {
			return m.Write(ctx, inode, 0, 0, Slice{Id: 1, Size: 4096, Len: 4096}, time.Now())
		}},
		{"Truncate", func() syscall.Errno { return m.Truncate(ctx, inode, 0, 8192, &attr, false) }},
		{"SetAttr", func() syscall.Errno { return m.SetAttr(ctx, inode, SetAttrMode, 0, &Attr{Mode: 0600}) }},
		{"Fallocate", func() syscall.Errno {
			var flen uint64
			return m.Fallocate(ctx, inode, 0, 0, 4096, &flen)
		}},
	}
	for _, c := range cases {
		if st := c.run(); st != syscall.EROFS {
			t.Errorf("%s through a handle opened before the fence = %v, want EROFS", c.name, st)
		}
	}

	// Reads through the same handle keep working: the fence revokes the right
	// to mutate, not the right to exist.
	if st := m.GetAttr(ctx, inode, &attr); st != 0 {
		t.Errorf("GetAttr after the fence = %v, want success", st)
	}
}

// TestTheLeaseExpiryRefusesWritesWithoutAFence is PLO-323 F-5: the deadline is
// re-checked by the write path itself rather than by a one-second ticker.
//
// The instant armed here is the lease EXPIRY, not `expiry − margin`. The margin
// is the tail of the lease reserved for the flush and the durability barrier
// (pkg/plori/mount/lease.go), and the staged writeback drains through Write
// during it — sealing at the margin would make the bounded flush window
// threat-model.md §7.5 mandates impossible. The margin remains the supervisor's
// trigger to stop the mount and start that drain.
func TestTheLeaseExpiryRefusesWritesWithoutAFence(t *testing.T) {
	m, inode := fencedTestMeta(t)
	ctx := Background()
	t.Cleanup(func() { ploriWriteExpiry.Store(noWriteExpiry) })

	slice := Slice{Id: 1, Size: 4096, Len: 4096}

	// A lease with time left on it: writes are submitted normally.
	PloriSetWriteExpiry(time.Now().Add(time.Hour))
	if st := m.Write(ctx, inode, 0, 0, slice, time.Now()); st != 0 {
		t.Fatalf("Write inside the lease = %v, want success", st)
	}

	// The same process, one instant past its expiry — the frozen-and-thawed
	// writer of threat-model.md §7.2. Nothing called PloriFenceWrites; the
	// deadline alone must stop it, and it must stop it on the call itself
	// rather than whenever a timer next fires.
	PloriSetWriteExpiry(time.Now().Add(-time.Millisecond))
	if PloriWritesFenced() {
		t.Fatal("this test must exercise the deadline, not the fence")
	}
	if st := m.Write(ctx, inode, 0, 0, slice, time.Now()); st != syscall.EROFS {
		t.Errorf("Write past the lease expiry = %v, want EROFS", st)
	}
	if st := m.Truncate(ctx, inode, 0, 8192, &Attr{}, false); st != syscall.EROFS {
		t.Errorf("Truncate past the lease expiry = %v, want EROFS", st)
	}
	if st := m.Mkdir(ctx, RootInode, "after-expiry", 0755, 022, 0, &inode2Discard, &Attr{}); st != syscall.EROFS {
		t.Errorf("Mkdir past the lease expiry = %v, want EROFS", st)
	}
}
