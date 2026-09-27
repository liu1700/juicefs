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
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type quotaAdmissionTest struct {
	m          Meta
	calls      atomic.Int32
	prefetches atomic.Int32
}

func (a *quotaAdmissionTest) Admit(context.Context) syscall.Errno {
	a.calls.Add(1)
	if err := PloriApplyGrant(a.m, int64(a.m.GetFormat().Capacity)+(64<<20), 65536); err != nil {
		return syscall.EIO
	}
	return 0
}
func (a *quotaAdmissionTest) Proactive() { a.prefetches.Add(1) }

// PLO-324's premise — "the worker applies a new grant from the lease renewal
// without a remount" — rests on three claims about the metadata engine that
// nobody had checked. The ADR says as much: "Increment is provisional until
// this exists". These tests check them.
//
//  1. The ceiling is read per operation, from an atomic pointer, so storing a
//     new Format changes the answer immediately (getFormat, base.go).
//  2. Exceeding the VOLUME ceiling answers ENOSPC, NOT EDQUOT. EDQUOT is the
//     user, group and directory quotas beside it (quota.go checkQuota). The
//     grant hook must therefore key off ENOSPC.
//  3. The in-memory Format is overwritten from the engine on every heartbeat
//     (base.go refresh: `m.Load(false)` then setFormat), so an in-memory-only
//     grant silently reverts within one heartbeat — 300 s on the Plori profile.

// openQuotaVolume creates a fresh SQLite volume with an explicit ceiling and an
// open session.
func openQuotaVolume(t *testing.T, capacity, inodes uint64) (*dbMeta, string) {
	t.Helper()
	return openQuotaVolumeWithTrash(t, capacity, inodes, 0)
}

func openQuotaVolumeWithTrash(t *testing.T, capacity, inodes uint64, trashDays int) (*dbMeta, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "quota.db")
	m, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	format := testFormat()
	format.Capacity = capacity
	format.Inodes = inodes
	format.TrashDays = trashDays
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("open session: %s", err)
	}
	db, ok := m.(*dbMeta)
	if !ok {
		t.Fatalf("meta is %T, want *dbMeta", m)
	}
	return db, dbPath
}

// fill charges `bytes` against the volume the way a write does, through the
// same counter checkQuota reads.
func fill(m *dbMeta, bytes int64) {
	atomic.StoreInt64(&m.usedSpace, bytes)
}

// TestTheVolumeCeilingAnswersENOSPCNotEDQUOT is the finding that changes the
// design of the grow hook.
//
// PLO-324 and quota-allocator.md §5 both describe the trigger as "EDQUOT". The
// engine does not agree: checkQuota returns ENOSPC for Format.Capacity and
// Format.Inodes, and reserves EDQUOT for the user, group and directory quotas
// (quota.go). A hook that watched for EDQUOT would never fire on the ceiling
// the control-plane's grant actually sets.
func TestTheVolumeCeilingAnswersENOSPCNotEDQUOT(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	ctx := Background()

	if st := m.checkQuota(ctx, 4<<20, 1, 0, 0); st != 0 {
		t.Fatalf("a write inside the ceiling was refused: %s", st)
	}
	fill(m, 8<<20)
	if st := m.checkQuota(ctx, 4<<20, 1, 0, 0); st != syscall.ENOSPC {
		t.Errorf("over the byte ceiling = %v, want ENOSPC (EDQUOT is the user/group/dir quota)", st)
	}

	fill(m, 0)
	atomic.StoreInt64(&m.usedInodes, 1024)
	if st := m.checkQuota(ctx, 0, 1, 0, 0); st != syscall.ENOSPC {
		t.Errorf("over the inode ceiling = %v, want ENOSPC", st)
	}
}

// TestPloriAdmissionRetriesTheOriginalAtomicMetadataOperation proves the
// marker is set only by Format.Capacity/Inodes and that the retry occurs after
// the engine transaction returned. This is the boundary VFS needs to retain an
// uploaded slice through a temporary allocation ceiling.
func TestPloriAdmissionRetriesTheOriginalAtomicMetadataOperation(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	ctx := Background()
	var ino Ino
	if st := m.Mknod(ctx, RootInode, "admit", TypeFile, 0644, 022, 0, "", &ino, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	fill(m, 8<<20)
	wrapped := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(wrapped, a)
	if st := wrapped.Truncate(ctx, ino, 0, 8192, &Attr{}, false); st != 0 {
		t.Fatalf("truncate after admission = %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
}

func TestPloriAdmissionCanGrowThroughSeveralGrants(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	var ino Ino
	if st := w.Create(Background(), RootInode, "large", 0644, 0, 0, &ino, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	var attr Attr
	if st := w.Truncate(Background(), ino, 0, 180<<20, &attr, false); st != 0 {
		t.Fatal(st)
	}
	if attr.Length != 180<<20 || a.calls.Load() != 3 {
		t.Fatalf("large operation: length=%d grants=%d", attr.Length, a.calls.Load())
	}
}

// A successful metadata operation crossing 80% must request headroom before
// any refusal. The admission waiter must not be involved in this fast path.
func TestPloriAdmissionPrefetchesAtEightyPercent(t *testing.T) {
	const capacity = 100 * 4096
	m, _ := openQuotaVolume(t, capacity, 1024)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	fill(m, 78*4096)
	atomic.StoreInt64(&m.newSpace, 0)
	var ino Ino
	if st := w.Mkdir(Background(), RootInode, "below", 0755, 0, 0, &ino, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	if got := a.prefetches.Load(); got != 0 {
		t.Fatalf("prefetch below threshold: %d", got)
	}
	if st := w.Mkdir(Background(), RootInode, "crosses", 0755, 0, 0, &ino, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	if got := a.prefetches.Load(); got != 1 {
		t.Fatalf("prefetch at threshold: %d", got)
	}
	if got := a.calls.Load(); got != 0 {
		t.Fatalf("a successful operation waited for admission: %d", got)
	}
}

// TestTheCeilingIsReadPerOperationSoAGrantNeedsNoRemount is claim 1: nothing
// caches the ceiling, so the operation immediately after PloriApplyGrant obeys
// the new number. Same process, same *dbMeta, same session — no reopen, no
// second metadata client.
func TestTheCeilingIsReadPerOperationSoAGrantNeedsNoRemount(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	ctx := Background()
	fill(m, 8<<20)

	if st := m.checkQuota(ctx, 4<<20, 1, 0, 0); st != syscall.ENOSPC {
		t.Fatalf("precondition: volume is not full (%v)", st)
	}
	sessionsBefore := sessionCount(t, m)
	sid := m.sid

	if err := PloriApplyGrant(m, 64<<20, 16384); err != nil {
		t.Fatalf("apply grant: %s", err)
	}
	if st := m.checkQuota(ctx, 4<<20, 1, 0, 0); st != 0 {
		t.Errorf("the operation after the grant was still refused: %s", st)
	}
	if got := m.GetFormat().Capacity; got != 64<<20 {
		t.Errorf("in-memory Capacity = %d, want %d", got, 64<<20)
	}

	// No second writer: the grant was applied by the process that already held
	// the session, which is what makes it safe on a metadata engine that
	// tolerates exactly one writer (ADR D3).
	if got := sessionCount(t, m); got != sessionsBefore {
		t.Errorf("sessions after the grant = %d, want %d — a second metadata client opened one", got, sessionsBefore)
	}
	if m.sid != sid {
		t.Errorf("session id changed from %d to %d — the grant restarted the session", sid, m.sid)
	}
}

// TestAGrantThatIsNotPersistedIsRevertedByTheHeartbeat is claim 3, and the
// reason PloriApplyGrant writes to the engine at all.
//
// base.go's refresh loop re-reads the stored Format every heartbeat and calls
// setFormat with it. The first half of this test does what an in-memory-only
// implementation would do and shows the reload undoing it; the second half does
// it through PloriApplyGrant and shows the reload confirming it.
func TestAGrantThatIsNotPersistedIsRevertedByTheHeartbeat(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)

	inMemoryOnly := *m.getFormat()
	inMemoryOnly.Capacity = 64 << 20
	m.setFormat(&inMemoryOnly)
	if got := m.GetFormat().Capacity; got != 64<<20 {
		t.Fatalf("precondition: in-memory Capacity = %d", got)
	}
	if _, err := m.Load(false); err != nil { // exactly what refresh() does
		t.Fatalf("reload: %s", err)
	}
	if got := m.GetFormat().Capacity; got != 8<<20 {
		t.Errorf("an in-memory-only ceiling survived the reload as %d; it must revert to %d", got, 8<<20)
	}

	if err := PloriApplyGrant(m, 64<<20, 16384); err != nil {
		t.Fatalf("apply grant: %s", err)
	}
	if _, err := m.Load(false); err != nil {
		t.Fatalf("reload: %s", err)
	}
	if got := m.GetFormat().Capacity; got != 64<<20 {
		t.Errorf("Capacity after the heartbeat reload = %d, want the granted %d", got, 64<<20)
	}
}

// TestAGrantOfZeroIsRefused is PLO-324's acceptance bullet "no failure mode
// converts missing/zero configuration into unlimited storage". JuiceFS reads
// Capacity == 0 as UNLIMITED, so the one value that must never be written is
// the one a dropped field, a zero-valued struct and a decode failure all
// produce.
func TestAGrantOfZeroIsRefused(t *testing.T) {
	for _, c := range []struct {
		name          string
		bytes, inodes int64
	}{
		{"both zero", 0, 0},
		{"zero bytes", 0, 16384},
		{"zero inodes", 64 << 20, 0},
		{"negative bytes", -1, 16384},
		{"negative inodes", 64 << 20, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, _ := openQuotaVolume(t, 8<<20, 1024)
			if err := PloriApplyGrant(m, c.bytes, c.inodes); err == nil {
				t.Fatalf("a grant of %d bytes / %d inodes was accepted", c.bytes, c.inodes)
			}
			if got := m.GetFormat().Capacity; got != 8<<20 {
				t.Errorf("Capacity = %d after a refused grant, want the previous %d", got, 8<<20)
			}
			if got := m.GetFormat().Inodes; got != 1024 {
				t.Errorf("Inodes = %d after a refused grant, want the previous %d", got, 1024)
			}
		})
	}
}

// TestTheGrantNeverWritesACredentialIntoTheReplicatedDatabase pins the reason
// PloriApplyGrant re-reads the stored Format instead of persisting the
// in-memory one: the in-memory one has been through the storage credential
// patch, and Litestream replicates whatever is in this database
// (threat-model F-11).
func TestTheGrantNeverWritesACredentialIntoTheReplicatedDatabase(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)

	patched := *m.getFormat()
	patched.AccessKey = "AKIAEXAMPLE"
	patched.SecretKey = "s3cr3t"
	patched.SessionToken = "token"
	m.setFormat(&patched)

	if err := PloriApplyGrant(m, 64<<20, 16384); err != nil {
		t.Fatalf("apply grant: %s", err)
	}
	stored, err := m.Load(false)
	if err != nil {
		t.Fatalf("reload: %s", err)
	}
	if stored.AccessKey != "" || stored.SecretKey != "" || stored.SessionToken != "" {
		t.Errorf("the grant persisted a credential: access_key=%q secret=%q token=%q",
			stored.AccessKey, stored.SecretKey, stored.SessionToken)
	}
}

// TestOnlyTheVolumeCeilingCountsAsAQuotaTrip guards the signal the supervisor
// turns into a Grow request. A refusal the allocator cannot fix — or no refusal
// at all — must not move the counter, or a mount would ask for capacity on
// every successful write.
func TestOnlyTheVolumeCeilingCountsAsAQuotaTrip(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	ctx := Background()

	before := PloriVolumeQuotaTrips()
	if st := m.checkQuota(ctx, 1<<20, 1, 0, 0); st != 0 {
		t.Fatalf("a write inside the ceiling was refused: %s", st)
	}
	if st := m.checkQuota(ctx, 0, 0, 0, 0); st != 0 {
		t.Fatalf("a no-op quota check was refused: %s", st)
	}
	if got := PloriVolumeQuotaTrips(); got != before {
		t.Fatalf("successful checks moved the trip counter by %d", got-before)
	}

	fill(m, 8<<20)
	if st := m.checkQuota(ctx, 1<<20, 1, 0, 0); st != syscall.ENOSPC {
		t.Fatalf("precondition: %v", st)
	}
	if got := PloriVolumeQuotaTrips(); got != before+1 {
		t.Errorf("trip counter moved by %d on one refusal, want 1", got-before)
	}
}

// TestPloriApplyGrantCostsOneMetadataWrite is PLO-346 §8's "live quota-update
// cost" gate, metadata half. The object-store half is measured through the real
// VFS in pkg/vfs (TestPloriGrantCostsNoObjectRequests): applying a grant reaches
// no object storage at all, so the only cost is here.
//
// Reported rather than asserted against a threshold: the number belongs in
// quota-allocator.md §8, and a wall-clock assertion in a unit test on shared CI
// is a flake, not a gate. What IS asserted is the shape — one write
// transaction's worth of WAL, not a rewrite of the database.
func TestPloriApplyGrantCostsOneMetadataWrite(t *testing.T) {
	m, dbPath := openQuotaVolume(t, 8<<20, 1024)
	const iterations = 50

	// Checkpoint everything the setup wrote, so the WAL measured below carries
	// only what the grants put there.
	if _, err := m.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("checkpoint: %s", err)
	}
	walBefore := fileSize(t, dbPath+"-wal")
	dbBefore := fileSize(t, dbPath)

	start := time.Now()
	for i := range iterations {
		// A distinct ceiling each time: an identical one would still write, but
		// a moving one is what production does.
		if err := PloriApplyGrant(m, int64(64<<20)+int64(i)*(64<<20), 16384+int64(i)); err != nil {
			t.Fatalf("apply grant %d: %s", i, err)
		}
	}
	elapsed := time.Since(start)
	walAfter := fileSize(t, dbPath+"-wal")
	dbAfter := fileSize(t, dbPath)

	perGrant := elapsed / iterations
	walPerGrant := (walAfter - walBefore) / iterations
	t.Logf("live grant application: %v per grant, %d bytes of WAL per grant, main db %+d bytes over %d grants, 0 object requests",
		perGrant, walPerGrant, dbAfter-dbBefore, iterations)

	// The main database file must not grow: the grant is an UPDATE of one row
	// of `setting`, and everything it produces lands in the WAL until a
	// checkpoint. A growing main file would mean the grant is doing structural
	// work (a table rebuild) that would multiply Litestream's replication cost.
	if dbAfter != dbBefore {
		t.Errorf("the main database grew by %d bytes over %d grants; a grant must be one row update",
			dbAfter-dbBefore, iterations)
	}
	if walPerGrant <= 0 {
		t.Errorf("no WAL was written for %d grants; the ceiling cannot be surviving a reload", iterations)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat %s: %s", path, err)
	}
	return fi.Size()
}

func sessionCount(t *testing.T, m Meta) int {
	t.Helper()
	sessions, err := m.ListSessions()
	if err != nil {
		t.Fatalf("list sessions: %s", err)
	}
	return len(sessions)
}

func ploriPending(m *dbMeta) (int64, int64) {
	m.volMu.Lock()
	defer m.volMu.Unlock()
	return m.pendingSpace, m.pendingInodes
}

// ploriGrantExactly leaves the volume with no free space for the next growth.
func ploriGrantExactly(t *testing.T, m *dbMeta, extra int64) {
	t.Helper()
	used, _ := m.volumeClaimed()
	if err := PloriApplyGrant(m, used+extra, 65536); err != nil {
		t.Fatalf("grant: %s", err)
	}
}

// claimsReleasedAdmission proves each wait happens with no claim outstanding
// and no metadata lock held: it applies the grant itself, which needs the
// SQLite write lock.
type claimsReleasedAdmission struct {
	m       *dbMeta
	calls   atomic.Int32
	pending atomic.Int64
}

func (a *claimsReleasedAdmission) Admit(context.Context) syscall.Errno {
	a.calls.Add(1)
	s, i := ploriPending(a.m)
	a.pending.Add(s + i)
	if err := PloriApplyGrant(a.m, int64(a.m.GetFormat().Capacity)+(64<<20), 65536); err != nil {
		return syscall.EIO
	}
	return 0
}
func (a *claimsReleasedAdmission) Proactive() {}

func TestPloriAdmissionWaitsWithNoClaimOutstanding(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	ctx := Background()
	var ino Ino
	if st := m.Mknod(ctx, RootInode, "grow", TypeFile, 0644, 022, 0, "", &ino, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	ploriGrantExactly(t, m, 0)
	w := PloriWithQuotaAdmission(m)
	a := &claimsReleasedAdmission{m: m}
	PloriSetQuotaAdmission(w, a)
	if st := w.Fallocate(ctx, ino, 0, 0, 1<<20, nil); st != 0 {
		t.Fatalf("fallocate after admission: %s", st)
	}
	if a.calls.Load() != 1 || a.pending.Load() != 0 {
		t.Fatalf("admission calls=%d claims during the wait=%d, want 1 and 0", a.calls.Load(), a.pending.Load())
	}
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after the call = %d/%d", s, i)
	}
}

// Only the clone preflight is retried after admission: nothing exists yet.
func TestPloriAdmissionRetriesTheClonePreflight(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 65536)
	ctx := Background()
	src := cloneFenceMkdir(t, m, ctx, RootInode, "source")
	file := cloneFenceFile(t, m, ctx, src, "file")
	if st := m.Fallocate(ctx, file, 0, 0, 1<<20, nil); st != 0 {
		t.Fatal(st)
	}
	ploriGrantExactly(t, m, 4096)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	var count, total uint64
	if st := w.Clone(ctx, RootInode, src, RootInode, "copy", CLONE_MODE_PRESERVE_ATTR, 0, 1, &count, &total); st != 0 {
		t.Fatalf("clone after admission: %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
	cloneFencePresent(t, m, ctx, RootInode, "copy")
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after clone = %d/%d", s, i)
	}
}

// A clone transaction refused after the preflight must not reach admission:
// retrying would repeat a partially executed clone.
func TestPloriAdmissionNeverRetriesAPartialClone(t *testing.T) {
	m, _ := openQuotaVolume(t, 8<<20, 65536)
	setup := Background()
	src := cloneFenceMkdir(t, m, setup, RootInode, "source")
	f1 := cloneFenceFile(t, m, setup, src, "f1")
	f2 := cloneFenceFile(t, m, setup, src, "f2")
	for _, f := range []Ino{f1, f2} {
		if st := m.Fallocate(setup, f, 0, 0, 2*4096, nil); st != 0 {
			t.Fatal(st)
		}
	}
	const summary, growth = 4096 + 4*4096, 64 * 4096
	ploriGrantExactly(t, m, summary+growth)
	var grown atomic.Bool
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterCloneEntry: func(top bool) {
			if top && grown.CompareAndSwap(false, true) {
				if st := m.Fallocate(Background(), f2, 0, 0, 2*4096+growth, nil); st != 0 {
					t.Errorf("grow the source: %s", st)
				}
			}
		},
	})
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	var count, total uint64
	if st := w.Clone(setup, RootInode, src, RootInode, "copy", CLONE_MODE_PRESERVE_ATTR, 0, 1, &count, &total); st != syscall.ENOSPC {
		t.Fatalf("clone of a source that grew = %s, want ENOSPC", st)
	}
	if got := a.calls.Load(); got != 0 {
		t.Fatalf("admission calls = %d; a partial clone was retried", got)
	}
	cloneFenceAbsent(t, m, setup, RootInode, "copy")
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after refused clone = %d/%d", s, i)
	}
}

// The first deletion of the hour needs the trash bucket. At a full grant the
// unlink waits for admission and then files the entry in the trash.
func TestPloriAdmissionUnlinkWaitsForTheTrashBucket(t *testing.T) {
	m, _ := openQuotaVolumeWithTrash(t, 8<<20, 65536, 1)
	ctx := Background()
	victim := cloneFenceFile(t, m, ctx, RootInode, "victim")
	ploriGrantExactly(t, m, 0)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	bucket := TrashBucketName(time.Now())
	if st := w.Unlink(ctx, RootInode, "victim"); st != 0 {
		t.Fatalf("unlink after admission: %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
	cloneFenceAbsent(t, m, ctx, RootInode, "victim")
	if TrashBucketName(time.Now()) != bucket {
		t.Skip("the hour changed during the test")
	}
	bucketIno := cloneFencePresent(t, m, ctx, TrashInode, bucket)
	cloneFencePresent(t, m, ctx, bucketIno, TrashEntryName(RootInode, victim, "victim"))
}

// Remove is not one transaction. A refused bucket stops it before anything is
// deleted here; after admission the retry deletes the tree into the trash.
func TestPloriAdmissionRemoveResumesAfterTheBucketGrant(t *testing.T) {
	m, _ := openQuotaVolumeWithTrash(t, 8<<20, 65536, 1)
	ctx := Background()
	tree := cloneFenceMkdir(t, m, ctx, RootInode, "tree")
	cloneFenceFile(t, m, ctx, tree, "a")
	cloneFenceFile(t, m, ctx, tree, "b")
	ploriGrantExactly(t, m, 0)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	var count uint64
	if st := w.Remove(ctx, RootInode, "tree", false, 1, &count); st != 0 {
		t.Fatalf("remove after admission: %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
	cloneFenceAbsent(t, m, ctx, RootInode, "tree")
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after remove = %d/%d", s, i)
	}
}

// ploriNestedTree builds tree/child/grandchild/leaf plus tree/child/sibling.
// Removed with one thread, child runs in emptyDir's goroutine and grandchild
// on its synchronous path, whose failure cancels the context it was given.
func ploriNestedTree(t *testing.T, m *dbMeta, ctx Context) {
	t.Helper()
	tree := cloneFenceMkdir(t, m, ctx, RootInode, "tree")
	child := cloneFenceMkdir(t, m, ctx, tree, "child")
	grandchild := cloneFenceMkdir(t, m, ctx, child, "grandchild")
	cloneFenceFile(t, m, ctx, grandchild, "leaf")
	cloneFenceFile(t, m, ctx, child, "sibling")
}

// The bucket refusal happens deep in the tree and emptyDir cancels its context
// to stop sibling work. That cancellation belongs to the attempt: the caller's
// context survives, the call waits for the grant and the retry removes the
// rest. Before attempts were scoped it canceled the caller and the retry ended
// in EINTR.
func TestPloriAdmissionNestedRemoveResumesAfterTheBucketGrant(t *testing.T) {
	m, _ := openQuotaVolumeWithTrash(t, 8<<20, 65536, 1)
	ctx := Background()
	ploriNestedTree(t, m, ctx)
	ploriGrantExactly(t, m, 0)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	var count uint64
	if st := w.Remove(ctx, RootInode, "tree", false, 1, &count); st != 0 {
		t.Fatalf("nested remove after admission: %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
	if ctx.Canceled() {
		t.Fatal("the operation canceled its caller's context")
	}
	cloneFenceAbsent(t, m, ctx, RootInode, "tree")
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after remove = %d/%d", s, i)
	}
	if st := w.Remove(ctx, RootInode, "missing", false, 1, &count); st != syscall.ENOENT || a.calls.Load() != 1 {
		t.Fatalf("remove of a missing name = %s after %d admissions, want ENOENT with no admission", st, a.calls.Load())
	}
}

// A caller canceled while its Remove is refused is not waited on or retried,
// even though the refusal is the ceiling's.
func TestPloriAdmissionRemoveStopsForACanceledCaller(t *testing.T) {
	m, _ := openQuotaVolumeWithTrash(t, 8<<20, 65536, 1)
	setup := Background()
	ploriNestedTree(t, m, setup)
	ploriGrantExactly(t, m, 0)
	w := PloriWithQuotaAdmission(m)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	ctx := Background()
	hook := volumeQuotaHook
	volumeQuotaHook = func(c Context) {
		hook(c)
		ctx.Cancel() // the caller goes away at the moment of the refusal
	}
	t.Cleanup(func() { volumeQuotaHook = hook })
	var count uint64
	if st := w.Remove(ctx, RootInode, "tree", false, 1, &count); st != syscall.EINTR {
		t.Fatalf("remove for a canceled caller = %s, want EINTR", st)
	}
	if got := a.calls.Load(); got != 0 {
		t.Fatalf("admission calls = %d for a canceled caller", got)
	}
	cloneFencePresent(t, m, setup, RootInode, "tree")
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after canceled remove = %d/%d", s, i)
	}
}

// openSliceDataVolume creates a SQLite volume and opens it the way
// plori-mount's Serve does: PloriWithQuotaBasis before NewSession.
func openSliceDataVolume(t *testing.T, capacity, inodes uint64) (*dbMeta, Meta) {
	t.Helper()
	m, err := newSQLMeta("sqlite3", filepath.Join(t.TempDir(), "slicedata.db"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	format := testFormat()
	format.Capacity = capacity
	format.Inodes = inodes
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	w, err := PloriWithQuotaBasis(m, QuotaBasisSliceData)
	if err != nil {
		t.Fatalf("slice_data basis: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("open session: %s", err)
	}
	return m.(*dbMeta), w
}

// An overwrite inside the file length is refused at the ceiling in
// slice_data mode, waits for a grant and is retried once.
func TestPloriQuotaSliceDataAdmissionRetriesARefusedOverwrite(t *testing.T) {
	m, w := openSliceDataVolume(t, 64<<20, 65536)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	f := dsFile(t, w, RootInode, "f")
	if st := dsWrite(w, Background(), f, 0, 4<<20); st != 0 {
		t.Fatal(st)
	}
	ploriGrantExactly(t, m, 0)
	var attr Attr
	if st := m.GetAttr(Background(), f, &attr); st != 0 || attr.Length != 4<<20 {
		t.Fatalf("setup: getattr %s length %d", st, attr.Length)
	}
	if st := dsWrite(w, Background(), f, 0, 1<<20); st != 0 {
		t.Fatalf("overwrite after admission = %s", st)
	}
	if got := a.calls.Load(); got != 1 {
		t.Fatalf("admission calls = %d, want 1", got)
	}
	if got := m.dataSpace.Load(); got != 5<<20 {
		t.Fatalf("slice data %d, want %d", got, 5<<20)
	}
	if s, i := ploriPending(m); s != 0 || i != 0 {
		t.Fatalf("pending after the call = %d/%d", s, i)
	}
}

// The 80 percent trigger reads the slice data. Clones push the logical
// counter far past the ceiling without a prefetch; a write that takes the
// slice data past 80 percent prefetches.
func TestPloriQuotaSliceDataPrefetchUsesDataBytes(t *testing.T) {
	const capacity = 10 << 20
	m, w := openSliceDataVolume(t, capacity, 65536)
	a := &quotaAdmissionTest{m: m}
	PloriSetQuotaAdmission(w, a)
	f := dsFile(t, w, RootInode, "f")
	if st := dsWrite(w, Background(), f, 0, 4<<20); st != 0 {
		t.Fatal(st)
	}
	for i := 0; i < 5; i++ {
		if st := dsClone(w, Background(), RootInode, f, "copy"+string(rune('a'+i))); st != 0 {
			t.Fatalf("clone %d: %s", i, st)
		}
	}
	if logical := PloriLogicalBytes(w); logical <= capacity {
		t.Fatalf("setup: logical %d, want above the ceiling %d", logical, capacity)
	}
	if got := a.prefetches.Load(); got != 0 {
		t.Fatalf("prefetch below 80 percent of slice data: %d", got)
	}
	g := dsFile(t, w, RootInode, "g")
	if st := dsWrite(w, Background(), g, 0, 4<<20+512<<10); st != 0 {
		t.Fatal(st)
	}
	if got := a.prefetches.Load(); got != 1 {
		t.Fatalf("prefetch at 80 percent of slice data: %d", got)
	}
	if got := a.calls.Load(); got != 0 {
		t.Fatalf("a successful operation waited for admission: %d", got)
	}
}

func TestPloriQuotaBasisSelection(t *testing.T) {
	for _, basis := range []string{"", QuotaBasisLogical} {
		m, _ := openQuotaVolume(t, 8<<20, 1024)
		w, err := PloriWithQuotaBasis(m, basis)
		if err != nil || PloriQuotaBasis(w) != QuotaBasisLogical {
			t.Fatalf("basis %q: %v, reported %s", basis, err, PloriQuotaBasis(w))
		}
	}
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	if _, err := PloriWithQuotaBasis(m, "bogus"); err == nil {
		t.Fatal("an unknown basis was accepted")
	}
	if PloriQuotaBasis(m) != QuotaBasisLogical {
		t.Fatal("a refused basis changed the mode")
	}
	_, w := openSliceDataVolume(t, 8<<20, 1024)
	if PloriQuotaBasis(w) != QuotaBasisSliceData || PloriDataSpaceDrift() != 0 {
		t.Fatalf("slice_data mount reports basis %s drift %d", PloriQuotaBasis(w), PloriDataSpaceDrift())
	}
	f := dsFile(t, w, RootInode, "f")
	if st := dsWrite(w, Background(), f, 0, 1<<20); st != 0 {
		t.Fatal(st)
	}
	if st := dsClone(w, Background(), RootInode, f, "g"); st != 0 {
		t.Fatal(st)
	}
	var total, avail, iused, iavail uint64
	if st := w.StatFS(Background(), RootInode, &total, &avail, &iused, &iavail); st != 0 {
		t.Fatal(st)
	}
	if used := total - avail; used != 1<<20 {
		t.Fatalf("StatFS used %d, want the slice data %d", used, 1<<20)
	}
	if logical := PloriLogicalBytes(w); logical < 2<<20 {
		t.Fatalf("logical bytes %d, want both copies", logical)
	}
}

// slice_data is kept by the SQL engine only; any other engine fails the mount.
func TestPloriQuotaBasisRefusesRedis(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/8", testConfig())
	if err != nil {
		t.Fatalf("create redis meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := PloriWithQuotaBasis(m, QuotaBasisSliceData); err == nil {
		t.Fatal("slice_data was accepted on redis")
	}
	if PloriQuotaBasis(m) != QuotaBasisLogical {
		t.Fatal("a refused basis changed the mode")
	}
}
