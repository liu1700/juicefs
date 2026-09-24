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
	"syscall"
	"testing"
	"time"

	"xorm.io/xorm"
)

// PLO-407's premise is that the "empty trash" CTA can say how much emptying the trash
// would free. That rests on two claims about this engine, and the second one is the
// reason the first is worth reporting at all:
//
//  1. A delete RELEASES NOTHING while `TrashDays > 0`. The volume counter the ceiling is
//     enforced against keeps the bytes, so a user who deleted a gigabyte to make room is
//     no better off until the trash goes.
//  2. The trash is therefore a SUBSET of `used_bytes`, and PloriMeasureTrash counts it
//     the way the counter counts it — so the CTA's number is part of the number the card
//     already shows, never an addition to it.
//
// These tests check both by execution, against the SQLite engine the Plori profile ships.
//
// Like the file under test they carry no `plori` build tag (PLO-429). That is the guard,
// not an oversight: `make test.plori.sqlite` compiles ./pkg/meta's DEFAULT-build test
// package, so re-tagging plori_trash.go would break this file's compile in CI — before it
// could silently take PloriMeasureTrash away from the plain build that plori-runtime's
// storage-worker links. `make test.plori.meta` runs the bodies below under the release
// tag set, which is where the numbers are actually asserted.

// openTrashVolume is openQuotaVolume with the trash on. TrashDays 1 is the ADR B8
// minimum: physical deletion has to lag metadata replication for the crash-consistency
// protocol to hold, so no Plori volume is ever formatted with 0.
func openTrashVolume(t *testing.T) *dbMeta {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "trash.db")
	m, err := newSQLMeta("sqlite3", dbPath, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	format := testFormat()
	format.TrashDays = 1
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
	return db
}

// createFile makes a file of exactly `length` bytes in the metadata's eyes. The
// accounting this measures is metadata accounting — align4K(attr.Length) — so a
// truncate charges the volume exactly what a write of that size would.
func createFile(t *testing.T, m *dbMeta, parent Ino, name string, length uint64) Ino {
	t.Helper()
	ctx := Background()
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, parent, name, 0o644, 0, syscall.O_RDWR, &ino, &attr); st != 0 {
		t.Fatalf("create %s: %s", name, st)
	}
	if length > 0 {
		if st := m.Truncate(ctx, ino, 0, length, &attr, true); st != 0 {
			t.Fatalf("truncate %s: %s", name, st)
		}
	}
	return ino
}

// volumeUsed is the pair StatFS reports, which is what the mount turns into
// `used_bytes`/`used_inodes` in the usage report (cmd/plori_mount.go ploriVolume.Usage).
func volumeUsed(t *testing.T, m *dbMeta) (int64, int64) {
	t.Helper()
	var total, avail, iused, iavail uint64
	if st := m.StatFS(Background(), RootInode, &total, &avail, &iused, &iavail); st != 0 {
		t.Fatalf("statfs: %s", st)
	}
	return int64(total - avail), int64(iused)
}

// TestADeleteReleasesNothingAndTheTrashIsInsideUsedBytes is claim 1 and claim 2 in one
// run, because they are one measurement taken twice.
func TestADeleteReleasesNothingAndTheTrashIsInsideUsedBytes(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	const size = 12 << 10 // 12 KiB: three 4 KiB blocks, so align4K is not a no-op
	createFile(t, m, RootInode, "notes.md", size)
	beforeBytes, beforeInodes := volumeUsed(t, m)

	if st := m.Unlink(ctx, RootInode, "notes.md"); st != 0 {
		t.Fatalf("unlink: %s", st)
	}
	afterBytes, afterInodes := volumeUsed(t, m)

	// Claim 1. Not "less than before" — NOT LESS AT ALL. The hour bucket the engine
	// created to hold the entry is itself a directory, so the volume ends up holding
	// MORE than it did before the user deleted the file (base.go checkTrash calls
	// updateStats(align4K(0), 1)). PLO-335 measured the same shape on its own fixture:
	// 20480 B / 5 inodes became 24576 B / 6.
	if afterBytes < beforeBytes {
		t.Errorf("a delete released %d bytes; with trash-days >= 1 it must release none",
			beforeBytes-afterBytes)
	}
	if afterBytes != beforeBytes+align4K(0) || afterInodes != beforeInodes+1 {
		t.Errorf("after the delete: %d B / %d inodes, want %d / %d (the file kept, plus one hour bucket)",
			afterBytes, afterInodes, beforeBytes+align4K(0), beforeInodes+1)
	}

	u, err := PloriMeasureTrash(m, ctx, 0)
	if err != nil {
		t.Fatalf("measure trash: %s", err)
	}
	if u.Partial {
		t.Error("a two-entry trash reported itself partial")
	}
	// Claim 2, exactly: one hour bucket (a directory, one 4 KiB block) plus the file.
	wantBytes := align4K(0) + align4K(size)
	if u.Bytes != wantBytes || u.Inodes != 2 {
		t.Errorf("trash = %d B / %d inodes, want %d / 2", u.Bytes, u.Inodes, wantBytes)
	}
	// And the whole point: it is a slice of the number the card already shows.
	if u.Bytes > afterBytes || u.Inodes > afterInodes {
		t.Errorf("trash (%d B / %d inodes) exceeds the volume's own usage (%d / %d): the breakdown must be a subset",
			u.Bytes, u.Inodes, afterBytes, afterInodes)
	}
}

// TestBothTrashNamespacesAreCounted is why the CTA can promise one number for one
// button. A user sees "the trash"; the platform has two, and until PLO-399 collapses
// them a deleted file passes through both.
func TestBothTrashNamespacesAreCounted(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	// The panel's own soft-delete: an ordinary mkdir, then a rename into it. Nothing
	// here is a JuiceFS trash operation, which is exactly the point — these bytes are
	// invisible to `.trash` and still charged to the account.
	var undo Ino
	var attr Attr
	if st := m.Mkdir(ctx, RootInode, PloriTrashDirName, 0o700, 0, 0, &undo, &attr); st != 0 {
		t.Fatalf("mkdir %s: %s", PloriTrashDirName, st)
	}
	const softSize = 8 << 10
	createFile(t, m, RootInode, "draft.md", softSize)
	if st := m.Rename(ctx, RootInode, "draft.md", undo, "1700000000.ZHJhZnQubWQ", 0, nil, nil); st != 0 {
		t.Fatalf("soft-delete rename: %s", st)
	}

	// JuiceFS's own trash: a real unlink.
	const hardSize = 20 << 10
	createFile(t, m, RootInode, "log.txt", hardSize)
	if st := m.Unlink(ctx, RootInode, "log.txt"); st != 0 {
		t.Fatalf("unlink: %s", st)
	}

	u, err := PloriMeasureTrash(m, ctx, 0)
	if err != nil {
		t.Fatalf("measure trash: %s", err)
	}
	// /.plori-trash (dir) + its one entry + the .trash hour bucket (dir) + its one entry.
	wantBytes := align4K(0) + align4K(softSize) + align4K(0) + align4K(hardSize)
	if u.Bytes != wantBytes || u.Inodes != 4 {
		t.Errorf("both namespaces = %d B / %d inodes, want %d / 4", u.Bytes, u.Inodes, wantBytes)
	}

	used, _ := volumeUsed(t, m)
	if u.Bytes > used {
		t.Errorf("trash %d B exceeds used %d B", u.Bytes, used)
	}
}

// TestAnEmptyVolumeReportsNoTrashRatherThanFailing: an Agent that has never deleted
// anything has neither directory, and that is a zero, not an error. If it were an error
// the CTA would go quiet on exactly the accounts that are healthiest.
func TestAnEmptyVolumeReportsNoTrashRatherThanFailing(t *testing.T) {
	m := openTrashVolume(t)
	u, err := PloriMeasureTrash(m, Background(), 0)
	if err != nil {
		t.Fatalf("measure trash on a volume with no trash: %s", err)
	}
	if u.Bytes != 0 || u.Inodes != 0 || u.Partial {
		t.Errorf("empty volume = %+v, want a clean zero", u)
	}
}

// TestTheWalkStopsAtItsBudgetAndSaysSo. The budget is the only defence against an Agent
// that deleted a million files, and a floor reported as an amount is worse than no
// number: the card would offer to free a fraction of what is there.
func TestTheWalkStopsAtItsBudgetAndSaysSo(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	const files = 12
	for i := range files {
		name := fmt.Sprintf("f%02d", i)
		createFile(t, m, RootInode, name, 4<<10)
		if st := m.Unlink(ctx, RootInode, name); st != 0 {
			t.Fatalf("unlink %s: %s", name, st)
		}
	}

	full, err := PloriMeasureTrash(m, ctx, 0)
	if err != nil {
		t.Fatalf("measure trash: %s", err)
	}
	if full.Partial {
		t.Fatalf("a %d-entry trash reported itself partial under the default budget", files)
	}

	// A budget below the entry count: the numbers stop being an answer and say so.
	cut, err := PloriMeasureTrash(m, ctx, 4)
	if err != nil {
		t.Fatalf("measure trash with a small budget: %s", err)
	}
	if !cut.Partial {
		t.Error("the walk exhausted its budget without saying the result is partial")
	}
	if cut.Bytes >= full.Bytes {
		t.Errorf("a capped walk reported %d B, not less than the full %d B", cut.Bytes, full.Bytes)
	}
}

// TestHardLinksInTheTrashAreCountedOnce. Emptying a trash full of links to one file
// frees one file's blocks, and recordStat (base.go, the engine's own recomputation of
// the volume counter) de-duplicates for the same reason. A breakdown that did not would
// promise space that emptying cannot return.
func TestHardLinksInTheTrashAreCountedOnce(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	const size = 16 << 10
	ino := createFile(t, m, RootInode, "original", size)
	var attr Attr
	if st := m.Link(ctx, ino, RootInode, "copy", &attr); st != 0 {
		t.Fatalf("link: %s", st)
	}
	for _, name := range []string{"original", "copy"} {
		if st := m.Unlink(ctx, RootInode, name); st != 0 {
			t.Fatalf("unlink %s: %s", name, st)
		}
	}

	u, err := PloriMeasureTrash(m, ctx, 0)
	if err != nil {
		t.Fatalf("measure trash: %s", err)
	}
	// One hour bucket, and the file's blocks once however many names reached the trash.
	if u.Bytes != align4K(0)+align4K(size) {
		t.Errorf("trash = %d B, want %d: a hard link's blocks are counted once",
			u.Bytes, align4K(0)+align4K(size))
	}
}

// sliceDataBytes is the slice_data figure by its definition, SUM(chunk_ref.size) over
// the rows with refs > 0, so the assertions below compare against the definition
// rather than against a counter.
func sliceDataBytes(t *testing.T, m *dbMeta) int64 {
	t.Helper()
	var used int64
	if err := m.roTxn(Background(), func(s *xorm.Session) error {
		var refs []sliceRef
		if err := s.Where("refs > 0").Find(&refs); err != nil {
			return err
		}
		used = 0
		for _, r := range refs {
			used += int64(r.Size)
		}
		return nil
	}); err != nil {
		t.Fatalf("sum chunk_ref: %s", err)
	}
	return used
}

// writeTrashTestSlice writes one new slice of `size` bytes at offset `off` of chunk 0.
func writeTrashTestSlice(t *testing.T, m *dbMeta, ino Ino, off, size uint32) {
	t.Helper()
	ctx := Background()
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	if st := m.Write(ctx, ino, 0, off, Slice{Id: id, Size: size, Len: size}, time.Now()); st != 0 {
		t.Fatalf("write slice %d to inode %d: %s", id, ino, st)
	}
}

// measureSliceDataTrash measures the trash on the slice_data basis and checks the
// subset rule against the slice_data figure at the same point.
func measureSliceDataTrash(t *testing.T, m *dbMeta) PloriTrashUsage {
	t.Helper()
	u, err := PloriMeasureTrashSliceData(m, Background(), 0)
	if err != nil {
		t.Fatalf("measure trash: %s", err)
	}
	if used := sliceDataBytes(t, m); u.Bytes > used {
		t.Fatalf("trash %d B exceeds the slice_data used bytes %d B: the breakdown must be a subset", u.Bytes, used)
	}
	return u
}

// TestSliceDataTrashCountsOnlySlicesNoLiveFileReferences: on the slice_data basis a
// trash file's length says nothing about what purging it releases. A clone shares
// the source's slices, so the source in the trash releases nothing while the clone
// is live, and releases everything once the clone is in the trash too.
func TestSliceDataTrashCountsOnlySlicesNoLiveFileReferences(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	const shared, overwrite, own = 64 << 10, 16 << 10, 8 << 10
	src := createFile(t, m, RootInode, "source", 0)
	writeTrashTestSlice(t, m, src, 0, shared)
	var count, total uint64
	if st := m.Clone(ctx, RootInode, src, RootInode, "copy", 0, 022, 4, &count, &total); st != 0 {
		t.Fatalf("clone: %s", st)
	}
	var cp Ino
	var attr Attr
	if st := m.Lookup(ctx, RootInode, "copy", &cp, &attr, false); st != 0 {
		t.Fatalf("lookup copy: %s", st)
	}
	// The copy diverges: a new slice over its first 16 KiB. The shared slice stays in
	// its chunk list, shadowed.
	writeTrashTestSlice(t, m, cp, 0, overwrite)
	other := createFile(t, m, RootInode, "other", 0)
	writeTrashTestSlice(t, m, other, 0, own)

	if used := sliceDataBytes(t, m); used != shared+overwrite+own {
		t.Fatalf("slice_data used = %d B, want %d: the clone must count its shared slice once", used, shared+overwrite+own)
	}
	if u := measureSliceDataTrash(t, m); u.Bytes != 0 || u.Inodes != 0 {
		t.Fatalf("empty trash = %+v, want zero", u)
	}

	if st := m.Unlink(ctx, RootInode, "source"); st != 0 {
		t.Fatalf("unlink source: %s", st)
	}
	if u := measureSliceDataTrash(t, m); u.Bytes != 0 {
		t.Errorf("source in the trash reports %d B; its only slice is still referenced by the live copy, so a purge releases 0", u.Bytes)
	}

	if st := m.Unlink(ctx, RootInode, "other"); st != 0 {
		t.Fatalf("unlink other: %s", st)
	}
	if u := measureSliceDataTrash(t, m); u.Bytes != own {
		t.Errorf("trash = %d B, want %d: only the unshared file is released", u.Bytes, own)
	}

	if st := m.Unlink(ctx, RootInode, "copy"); st != 0 {
		t.Fatalf("unlink copy: %s", st)
	}
	sliced := measureSliceDataTrash(t, m)
	if want := int64(shared + overwrite + own); sliced.Bytes != want {
		t.Errorf("trash = %d B, want %d: with every reference in the trash every slice is released", sliced.Bytes, want)
	}

	// The logical walk over the same trash is unchanged: lengths, not slices.
	logical, err := PloriMeasureTrash(m, ctx, 0)
	if err != nil {
		t.Fatalf("measure trash on the logical basis: %s", err)
	}
	if logical.Inodes != sliced.Inodes || logical.Partial != sliced.Partial {
		t.Errorf("logical %+v and slice_data %+v disagree on inodes or partial", logical, sliced)
	}
	buckets := logical.Inodes - 3
	if want := buckets*align4K(0) + 2*align4K(shared) + align4K(own); logical.Bytes != want {
		t.Errorf("logical trash = %d B, want %d (lengths of the three files plus %d hour buckets)", logical.Bytes, want, buckets)
	}
}

// TestSliceDataTrashExcludesAHardLinkWithALiveName: a purge removes the trash names,
// and a file that still has a name outside the trash keeps its chunk lists.
func TestSliceDataTrashExcludesAHardLinkWithALiveName(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	const size = 32 << 10
	ino := createFile(t, m, RootInode, "original", 0)
	writeTrashTestSlice(t, m, ino, 0, size)
	var attr Attr
	if st := m.Link(ctx, ino, RootInode, "link", &attr); st != 0 {
		t.Fatalf("link: %s", st)
	}
	if st := m.Unlink(ctx, RootInode, "link"); st != 0 {
		t.Fatalf("unlink link: %s", st)
	}
	if u := measureSliceDataTrash(t, m); u.Bytes != 0 {
		t.Errorf("trash = %d B with a live name left; a purge releases 0", u.Bytes)
	}
	if st := m.Unlink(ctx, RootInode, "original"); st != 0 {
		t.Fatalf("unlink original: %s", st)
	}
	if u := measureSliceDataTrash(t, m); u.Bytes != size {
		t.Errorf("trash = %d B with every name in the trash, want %d", u.Bytes, size)
	}
}

// TestSliceDataTrashPartialWalkIsAFloor: a capped walk sees fewer trash references,
// so it reports no more than the full walk and says it is partial.
func TestSliceDataTrashPartialWalkIsAFloor(t *testing.T) {
	m := openTrashVolume(t)
	ctx := Background()

	for i := range 8 {
		name := fmt.Sprintf("f%02d", i)
		ino := createFile(t, m, RootInode, name, 0)
		writeTrashTestSlice(t, m, ino, 0, 4<<10)
		if st := m.Unlink(ctx, RootInode, name); st != 0 {
			t.Fatalf("unlink %s: %s", name, st)
		}
	}
	full := measureSliceDataTrash(t, m)
	if full.Partial || full.Bytes != 8*(4<<10) {
		t.Fatalf("full walk = %+v, want %d B and not partial", full, 8*(4<<10))
	}
	cut, err := PloriMeasureTrashSliceData(m, ctx, 4)
	if err != nil {
		t.Fatalf("measure trash with a small budget: %s", err)
	}
	if !cut.Partial || cut.Bytes >= full.Bytes {
		t.Errorf("capped walk = %+v, want partial and less than %d B", cut, full.Bytes)
	}
}
