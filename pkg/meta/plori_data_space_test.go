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
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"xorm.io/xorm"
)

// These tests cover QuotaBasisSliceData on the SQLite engine: the counter row
// ploriDataSpace, the in-memory dataSpace and a full recount of chunk_ref must
// agree after every reference change (plori_data_space.go).

const dsMiB = 1 << 20

// dsOpen formats a SQLite volume, switches it to slice_data and loads its
// counters without a session, so no background job interleaves.
func dsOpen(t *testing.T, capacity, inodes uint64, trashDays int) (*dbMeta, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ds.db")
	m, err := newSQLMeta("sqlite3", path, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	format := testFormat()
	format.Capacity = capacity
	format.Inodes = inodes
	format.TrashDays = trashDays
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init: %s", err)
	}
	db := m.(*dbMeta)
	if _, _, _, err := db.ploriEnableSliceData(); err != nil {
		t.Fatalf("enable slice_data: %s", err)
	}
	db.refreshUsage()
	return db, path
}

// dsReopen opens a second client on path, as a new mount process would.
func dsReopen(t *testing.T, path string) *dbMeta {
	t.Helper()
	m, err := newSQLMeta("sqlite3", path, testConfig())
	if err != nil {
		t.Fatalf("reopen meta: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := m.Load(true); err != nil {
		t.Fatalf("load format: %s", err)
	}
	return m.(*dbMeta)
}

func dsFile(t *testing.T, m Meta, parent Ino, name string) Ino {
	t.Helper()
	var ino Ino
	if st := m.Mknod(Background(), parent, name, TypeFile, 0644, 022, 0, "", &ino, nil); st != 0 {
		t.Fatalf("mknod %q: %s", name, st)
	}
	return ino
}

func dsDir(t *testing.T, m Meta, parent Ino, name string) Ino {
	t.Helper()
	var ino Ino
	if st := m.Mkdir(Background(), parent, name, 0755, 022, 0, &ino, nil); st != 0 {
		t.Fatalf("mkdir %q: %s", name, st)
	}
	return ino
}

func dsLookup(t *testing.T, m Meta, parent Ino, name string) Ino {
	t.Helper()
	var ino Ino
	if st := m.Lookup(Background(), parent, name, &ino, &Attr{}, false); st != 0 {
		t.Fatalf("lookup %q: %s", name, st)
	}
	return ino
}

// dsWrite writes one new slice of size bytes at off; the slice must stay
// inside one chunk.
func dsWrite(m Meta, ctx Context, ino Ino, off uint64, size uint32) syscall.Errno {
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		return st
	}
	indx, coff := uint32(off/ChunkSize), uint32(off%ChunkSize)
	if uint64(coff)+uint64(size) > ChunkSize {
		panic(fmt.Sprintf("slice of %d at %d crosses a chunk", size, off))
	}
	return m.Write(ctx, ino, indx, coff, Slice{Id: id, Size: size, Len: size}, time.Now())
}

func dsMustWrite(t *testing.T, m Meta, ino Ino, off uint64, size uint32) {
	t.Helper()
	if st := dsWrite(m, Background(), ino, off, size); st != 0 {
		t.Fatalf("write %d bytes at %d of %d: %s", size, off, ino, st)
	}
}

func dsClone(m Meta, ctx Context, srcParent, src Ino, name string) syscall.Errno {
	var count, total uint64
	return m.Clone(ctx, srcParent, src, RootInode, name, CLONE_MODE_PRESERVE_ATTR, 0, 1, &count, &total)
}

// dsDrain waits until no shadowed-slice release and no file data deletion is
// running, then deletes the data of files whose deletion was deferred. Write
// and Truncate schedule releases in a goroutine (plori_release.go). Unlink and
// trash purge delete file data in a goroutine holding a maxDeleting token, and
// defer it to the hourly job when all tokens are taken.
func dsDrain(t *testing.T, m *dbMeta) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for m.ploriReleasing.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d shadowed-slice releases still running", m.ploriReleasing.Load())
		}
		time.Sleep(time.Millisecond)
	}
	for len(m.maxDeleting) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d file deletions still running", len(m.maxDeleting))
		}
		time.Sleep(time.Millisecond)
	}
	files, err := m.doFindDeletedFiles(time.Now().Add(time.Hour).Unix(), 1e6)
	if err != nil {
		t.Fatalf("find deleted files: %s", err)
	}
	for ino, length := range files {
		m.doDeleteFileData(ino, length)
	}
}

// dsRecount is SUM(size) over refs > 0, read without writing the row.
func dsRecount(t *testing.T, m *dbMeta) int64 {
	t.Helper()
	var total int64
	if err := m.simpleTxn(Background(), func(s *xorm.Session) error {
		var err error
		total, err = s.Where("refs > 0").SumInt(&sliceRef{}, "size")
		return err
	}); err != nil {
		t.Fatalf("recount: %s", err)
	}
	return total
}

// dsRow is the persisted ploriDataSpace row and whether it exists.
func dsRow(t *testing.T, m *dbMeta) (int64, bool) {
	t.Helper()
	c := counter{Name: ploriDataSpace}
	var ok bool
	if err := m.simpleTxn(Background(), func(s *xorm.Session) error {
		var err error
		ok, err = s.Get(&c)
		return err
	}); err != nil {
		t.Fatalf("read counter: %s", err)
	}
	return c.Value, ok
}

// dsCheck drains deletions and requires memory, row and recount to agree, and
// to equal want unless want is negative.
func dsCheck(t *testing.T, m *dbMeta, want int64, step string) int64 {
	t.Helper()
	dsDrain(t, m)
	mem := m.dataSpace.Load()
	row, ok := dsRow(t, m)
	sum := dsRecount(t, m)
	if !ok || mem != row || row != sum {
		t.Fatalf("%s: memory %d, row %d (exists %t), recount %d must agree", step, mem, row, ok, sum)
	}
	if want >= 0 && sum != want {
		t.Fatalf("%s: slice data %d, want %d", step, sum, want)
	}
	return sum
}

// dsPurge empties the trash now, including the bucket of the current hour.
// A mount never removes the current bucket, whose inode checkTrash caches, so
// the test forgets the cached bucket as well.
func dsPurge(t *testing.T, m *dbMeta) {
	t.Helper()
	if st := m.doCleanupTrash(Background(), 1, true, nil); st != 0 {
		t.Fatalf("purge trash: %s", st)
	}
	m.Lock()
	m.subTrash = internalNode{}
	m.Unlock()
}

func dsLogical(m *dbMeta) int64 {
	return atomic.LoadInt64(&m.usedSpace) + atomic.LoadInt64(&m.newSpace)
}

// dsCanonical writes the 100 MiB canonical tree src/f and returns src.
func dsCanonical(t *testing.T, m *dbMeta) Ino {
	t.Helper()
	src := dsDir(t, m, RootInode, "src")
	f := dsFile(t, m, src, "f")
	dsMustWrite(t, m, f, 0, 64*dsMiB)
	dsMustWrite(t, m, f, 64*dsMiB, 36*dsMiB)
	return src
}

// dsCopies clones src three times and writes 1 MiB into each copy.
func dsCopies(t *testing.T, m *dbMeta, src Ino) {
	t.Helper()
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("copy%d", i)
		if st := dsClone(m, Background(), RootInode, src, name); st != 0 {
			t.Fatalf("clone %s: %s", name, st)
		}
	}
	dsCheck(t, m, 100*dsMiB, "after three clones")
	for i := 1; i <= 3; i++ {
		f := dsLookup(t, m, dsLookup(t, m, RootInode, fmt.Sprintf("copy%d", i)), "f")
		dsMustWrite(t, m, f, 0, dsMiB)
	}
}

// Design test (1): 100 MiB, three clones, 1 MiB written in each copy is
// 100 MiB + 3 MiB of slice data, while the logical counter holds four trees.
func TestDataSpaceCountsSharedCloneDataOnce(t *testing.T) {
	m, _ := dsOpen(t, 1<<30, 1<<20, 0)
	dsCheck(t, m, 0, "empty volume")
	src := dsCanonical(t, m)
	dsCheck(t, m, 100*dsMiB, "canonical tree")
	dsCopies(t, m, src)
	dsCheck(t, m, 103*dsMiB, "after 1 MiB edits in three copies")
	if logical := dsLogical(m); logical < 400*dsMiB {
		t.Fatalf("logical counter %d, want at least the four trees", logical)
	}
	var total, avail, iused, iavail uint64
	if st := m.StatFS(Background(), RootInode, &total, &avail, &iused, &iavail); st != 0 {
		t.Fatal(st)
	}
	if used := int64(total - avail); used != 103*dsMiB || total != 1<<30 {
		t.Fatalf("StatFS used %d of %d, want %d of %d", used, total, 103*dsMiB, 1<<30)
	}
}

// Design test (2): deleting and purging the canonical tree frees nothing the
// copies still share; deleting and purging every copy then frees everything.
func TestDataSpaceDeleteSourceThenCopies(t *testing.T) {
	m, _ := dsOpen(t, 1<<30, 1<<20, 1)
	src := dsCanonical(t, m)
	dsCopies(t, m, src)
	dsCheck(t, m, 103*dsMiB, "copies written")

	remove := func(name string) {
		var count uint64
		if st := m.Remove(Background(), RootInode, name, false, 1, &count); st != 0 {
			t.Fatalf("remove %s: %s", name, st)
		}
	}
	remove("src")
	dsCheck(t, m, 103*dsMiB, "canonical in trash")
	dsPurge(t, m)
	dsCheck(t, m, 103*dsMiB, "canonical purged")
	for i := 1; i <= 3; i++ {
		remove(fmt.Sprintf("copy%d", i))
	}
	dsCheck(t, m, 103*dsMiB, "copies in trash")
	dsPurge(t, m)
	dsCheck(t, m, 0, "copies purged")
}

// dsHarness drives a random sequence of reference changes over files in the
// root directory. Each op returns a description, or "" when it did nothing.
// plori_release_test.go adds compaction, shadowed release and delayed-slice
// cleanup to dsOps.
type dsHarness struct {
	t      *testing.T
	m      *dbMeta
	rnd    *rand.Rand
	files  []string
	slices map[string]int // slices written into each file's chunks, including inherited ones
	next   int
}

type dsOp struct {
	name string
	run  func(h *dsHarness) string
}

func (h *dsHarness) pick() (string, Ino, bool) {
	if len(h.files) == 0 {
		return "", 0, false
	}
	name := h.files[h.rnd.Intn(len(h.files))]
	return name, dsLookup(h.t, h.m, RootInode, name), true
}

func (h *dsHarness) newName() string {
	h.next++
	return fmt.Sprintf("f%d", h.next)
}

func (h *dsHarness) forget(name string) {
	for i, n := range h.files {
		if n == name {
			h.files = append(h.files[:i], h.files[i+1:]...)
			break
		}
	}
	delete(h.slices, name)
}

// dsMaxSlices keeps every chunk below the 99-slice compaction trigger of
// baseMeta.Write, which this unit does not exercise.
const dsMaxSlices = 60

var dsOps = []dsOp{
	{"create", func(h *dsHarness) string {
		name := h.newName()
		dsFile(h.t, h.m, RootInode, name)
		h.files = append(h.files, name)
		return "create " + name
	}},
	{"write", func(h *dsHarness) string {
		name, ino, ok := h.pick()
		if !ok || h.slices[name] >= dsMaxSlices {
			return ""
		}
		size := uint32(1+h.rnd.Intn(256)) << 12
		off := uint64(h.rnd.Intn(2))*ChunkSize + uint64(h.rnd.Intn(64))<<12
		dsMustWrite(h.t, h.m, ino, off, size)
		h.slices[name]++
		return fmt.Sprintf("write %d at %d of %s", size, off, name)
	}},
	{"clone", func(h *dsHarness) string {
		name, ino, ok := h.pick()
		if !ok {
			return ""
		}
		dst := h.newName()
		if st := dsClone(h.m, Background(), RootInode, ino, dst); st != 0 {
			h.t.Fatalf("clone %s to %s: %s", name, dst, st)
		}
		h.files = append(h.files, dst)
		h.slices[dst] = h.slices[name]
		return "clone " + name + " to " + dst
	}},
	{"copy_file_range", func(h *dsHarness) string {
		src, in, ok := h.pick()
		dst, out, _ := h.pick()
		if !ok || in == out || h.slices[dst]+h.slices[src] >= dsMaxSlices {
			return ""
		}
		size := uint64(1+h.rnd.Intn(512)) << 12
		var copied, length uint64
		if st := h.m.CopyFileRange(Background(), in, 0, out, uint64(h.rnd.Intn(16))<<12, size, 0, &copied, &length); st != 0 {
			h.t.Fatalf("copy_file_range %s to %s: %s", src, dst, st)
		}
		h.slices[dst] += h.slices[src]
		return fmt.Sprintf("copy_file_range %d of %s to %s", copied, src, dst)
	}},
	{"truncate", func(h *dsHarness) string {
		name, ino, ok := h.pick()
		if !ok {
			return ""
		}
		length := uint64(h.rnd.Intn(3*ChunkSize/2)) &^ 4095
		if st := h.m.Truncate(Background(), ino, 0, length, &Attr{}, false); st != 0 {
			h.t.Fatalf("truncate %s to %d: %s", name, length, st)
		}
		return fmt.Sprintf("truncate %s to %d", name, length)
	}},
	{"unlink", func(h *dsHarness) string {
		name, _, ok := h.pick()
		if !ok {
			return ""
		}
		if st := h.m.Unlink(Background(), RootInode, name); st != 0 {
			h.t.Fatalf("unlink %s: %s", name, st)
		}
		h.forget(name)
		return "unlink " + name
	}},
	{"purge", func(h *dsHarness) string {
		dsPurge(h.t, h.m)
		return "purge trash"
	}},
}

// Design test (5): after every step of a random sequence the maintained row,
// the in-memory value and the recount are equal. With trash on, released and
// compacted slices are held in delslices; with trash off their references are
// removed at once.
func TestDataSpaceRandomSequenceMatchesRecount(t *testing.T) {
	t.Run("trash on", func(t *testing.T) { dsRunRandom(t, 1) })
	t.Run("trash off", func(t *testing.T) { dsRunRandom(t, 0) })
}

func dsRunRandom(t *testing.T, trashDays int) {
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	m, _ := dsOpen(t, 0, 0, trashDays)
	h := &dsHarness{t: t, m: m, rnd: rand.New(rand.NewSource(seed)), slices: map[string]int{}}
	for step := 0; step < 300; step++ {
		op := dsOps[h.rnd.Intn(len(dsOps))]
		if what := op.run(h); what != "" {
			dsCheck(t, m, -1, fmt.Sprintf("step %d (%s)", step, what))
		}
	}
	for _, name := range append([]string(nil), h.files...) {
		if st := m.Unlink(Background(), RootInode, name); st != 0 {
			t.Fatalf("unlink %s: %s", name, st)
		}
	}
	dsPurge(t, m)
	dsCleanupDelayed(t, m)
	dsCheck(t, m, 0, "everything deleted and purged")
}

// Design test (6): the recount at open repairs a lost in-memory update and a
// wrong persisted row, reports the drift of the row, and creates the row on a
// volume last mounted in logical mode.
func TestDataSpaceRecountAtOpen(t *testing.T) {
	t.Run("memory update lost", func(t *testing.T) {
		m, path := dsOpen(t, 0, 0, 0)
		f := dsFile(t, m, RootInode, "f")
		dsMustWrite(t, m, f, 0, 3*dsMiB)
		dsMustWrite(t, m, f, 0, dsMiB)
		// A crash between the metadata commit and the memory update: the
		// row and the chunk_ref table hold the write, memory does not.
		m.dataSpace.Add(-dsMiB)
		if row, _ := dsRow(t, m); row != 4*dsMiB || m.dataSpace.Load() != 3*dsMiB {
			t.Fatalf("setup: row %d memory %d", row, m.dataSpace.Load())
		}
		_ = m.Shutdown()
		r := dsReopen(t, path)
		recount, drift, existed, err := r.ploriEnableSliceData()
		if err != nil || !existed || drift != 0 || recount != 4*dsMiB {
			t.Fatalf("reopen: recount %d drift %d existed %t err %v", recount, drift, existed, err)
		}
		dsCheck(t, r, 4*dsMiB, "after reopen")
	})
	t.Run("persisted row wrong", func(t *testing.T) {
		m, path := dsOpen(t, 0, 0, 0)
		dsMustWrite(t, m, dsFile(t, m, RootInode, "f"), 0, 2*dsMiB)
		if err := m.txn(func(s *xorm.Session) error {
			_, err := s.Exec(m.sqlConv(stmtDataAdd), -4096)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		_ = m.Shutdown()
		r := dsReopen(t, path)
		recount, drift, existed, err := r.ploriEnableSliceData()
		if err != nil || !existed || drift != 4096 || recount != 2*dsMiB {
			t.Fatalf("reopen: recount %d drift %d existed %t err %v", recount, drift, existed, err)
		}
		dsCheck(t, r, 2*dsMiB, "row rewritten")
	})
	t.Run("volume last mounted in logical mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "logical.db")
		lm, err := newSQLMeta("sqlite3", path, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := lm.Init(testFormat(), true); err != nil {
			t.Fatal(err)
		}
		l := lm.(*dbMeta)
		src := dsFile(t, l, RootInode, "f")
		dsMustWrite(t, l, src, 0, 5*dsMiB)
		if st := dsClone(l, Background(), RootInode, src, "g"); st != 0 {
			t.Fatal(st)
		}
		if _, ok := dsRow(t, l); ok || l.dataSpace.Load() != 0 {
			t.Fatal("logical mode must not create or maintain the ploriDataSpace row")
		}
		_ = l.Shutdown()
		r := dsReopen(t, path)
		recount, drift, existed, err := r.ploriEnableSliceData()
		if err != nil || existed || drift != 0 || recount != 5*dsMiB {
			t.Fatalf("first slice_data open: recount %d drift %d existed %t err %v", recount, drift, existed, err)
		}
		dsCheck(t, r, 5*dsMiB, "row created")
	})
}

// Logical mode leaves every admission and usage number on the logical basis.
func TestDataSpaceLogicalModeIsUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logical.db")
	lm, err := newSQLMeta("sqlite3", path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lm.Shutdown() })
	format := testFormat()
	format.Capacity = 8 * dsMiB
	if err := lm.Init(format, true); err != nil {
		t.Fatal(err)
	}
	l := lm.(*dbMeta)
	l.refreshUsage()
	f := dsFile(t, l, RootInode, "f")
	dsMustWrite(t, l, f, 0, 5*dsMiB)
	// An overwrite inside the length is free on the logical basis.
	before := dsLogical(l)
	dsMustWrite(t, l, f, 0, 5*dsMiB)
	if dsLogical(l) != before {
		t.Fatalf("logical overwrite changed usage %d -> %d", before, dsLogical(l))
	}
	if st := dsClone(l, Background(), RootInode, f, "g"); st != syscall.ENOSPC {
		t.Fatalf("logical clone past the ceiling = %s, want ENOSPC", st)
	}
	if _, ok := dsRow(t, l); ok || l.dataSpace.Load() != 0 {
		t.Fatal("logical mode must not create or maintain the ploriDataSpace row")
	}
}
