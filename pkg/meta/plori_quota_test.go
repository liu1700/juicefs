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
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

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
// open session, and zeroes the usage counters.
//
// The zeroing is not cosmetic. A fresh baseMeta starts at usedSpace ==
// usedInodes == unknownUsage (-1, base.go), and the loop that reads the real
// counters runs on the heartbeat ticker, so a test that did not do this would
// be measuring an off-by-one against a value production never has.
func openQuotaVolume(t *testing.T, capacity, inodes uint64) (*dbMeta, string) {
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
	atomic.StoreInt64(&db.usedSpace, 0)
	atomic.StoreInt64(&db.usedInodes, 0)
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
