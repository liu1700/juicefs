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
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"xorm.io/xorm"
)

// These tests cover the shadowed-slice release and the compaction data growth
// claim of QuotaBasisSliceData (plori_release.go, plori_release_sql.go).

func init() {
	dsOps = append(dsOps,
		dsOp{"compact", func(h *dsHarness) string {
			name, ino, ok := h.pick()
			if !ok {
				return ""
			}
			indx := uint32(h.rnd.Intn(2))
			dsObjectsNoop(h.m)
			h.m.compactChunk(ino, indx, false, true, 0)
			return fmt.Sprintf("compact chunk %d of %s", indx, name)
		}},
		dsOp{"release", func(h *dsHarness) string {
			name, ino, ok := h.pick()
			if !ok {
				return ""
			}
			indx := uint32(h.rnd.Intn(2))
			dsObjectsNoop(h.m)
			before := dsZeros(dsLayout(h.t, h.m, ino, indx))
			if st := h.m.doPloriReleaseShadowed(ino, indx); st != 0 {
				h.t.Fatalf("release %s chunk %d: %s", name, indx, st)
			}
			if after := dsZeros(dsLayout(h.t, h.m, ino, indx)); !reflect.DeepEqual(before, after) {
				h.t.Fatalf("release of %s chunk %d changed what reads see:\n%v\n%v", name, indx, before, after)
			}
			return fmt.Sprintf("release chunk %d of %s", indx, name)
		}},
		dsOp{"delayed cleanup", func(h *dsHarness) string {
			dsObjectsNoop(h.m)
			dsCleanupDelayed(h.t, h.m)
			return "delayed-slice cleanup"
		}},
	)
}

// dsObjectsNoop accepts the object store messages of compaction and slice
// deletion without an object store.
func dsObjectsNoop(m *dbMeta) {
	m.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
}

// dsCleanupDelayed removes every delslices hold now, as the job does after
// trash-days.
func dsCleanupDelayed(t *testing.T, m *dbMeta) {
	t.Helper()
	if _, err := m.doCleanupDelayedSlices(Background(), time.Now().Unix()+1); err != nil {
		t.Fatalf("cleanup delayed slices: %s", err)
	}
}

// dsLayout is what a read of the chunk sees.
func dsLayout(t *testing.T, m *dbMeta, ino Ino, indx uint32) []Slice {
	t.Helper()
	ss, st := m.doRead(Background(), ino, indx)
	if st != 0 {
		t.Fatalf("read %d chunk %d: %s", ino, indx, st)
	}
	return buildSlice(ss)
}

// dsZeros merges adjacent zero pieces (id 0) of a layout into one with offset
// 0. A read returns zeros for them whatever their offset and split, which
// depend on the order in which buildSlice cut the holes.
func dsZeros(layout []Slice) []Slice {
	var out []Slice
	for _, s := range layout {
		if s.Id == 0 {
			if n := len(out); n > 0 && out[n-1].Id == 0 {
				out[n-1].Len += s.Len
				out[n-1].Size += s.Len
				continue
			}
			s.Size, s.Off = s.Len, 0
		}
		out = append(out, s)
	}
	return out
}

// dsList is the raw chunk list.
func dsList(t *testing.T, m *dbMeta, ino Ino, indx uint32) []*slice {
	t.Helper()
	ss, st := m.doRead(Background(), ino, indx)
	if st != 0 {
		t.Fatalf("read %d chunk %d: %s", ino, indx, st)
	}
	return ss
}

func dsListBytes(ss []*slice) []byte {
	var b []byte
	for _, s := range ss {
		b = append(b, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	return b
}

func dsWriteID(t *testing.T, m Meta, ctx Context, ino Ino, off uint64, size uint32) (uint64, syscall.Errno) {
	t.Helper()
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	st := m.Write(ctx, ino, uint32(off/ChunkSize), uint32(off%ChunkSize), Slice{Id: id, Size: size, Len: size}, time.Now())
	return id, st
}

func TestPloriShadowed(t *testing.T) {
	sl := func(pos uint32, id uint64, n uint32) *slice {
		return &slice{pos: pos, id: id, size: n, len: n}
	}
	cases := []struct {
		name string
		ss   []*slice
		want []bool
	}{
		{"one slice", []*slice{sl(0, 1, 8192)}, nil},
		{"covered by a later slice", []*slice{sl(0, 1, 4096), sl(0, 2, 8192)}, []bool{true, false}},
		{"partly covered", []*slice{sl(0, 1, 8192), sl(0, 2, 4096)}, nil},
		{"covered by two later slices", []*slice{sl(4096, 1, 4096), sl(0, 2, 6144), sl(6144, 3, 4096)}, []bool{true, false, false}},
		{"covered by a truncate zero slice", []*slice{sl(0, 1, 8192), sl(0, 0, 8192)}, []bool{true, false}},
		{"earlier zero slice covered", []*slice{sl(0, 0, 8192), sl(0, 2, 8192)}, []bool{true, false}},
		{"repeated identical entry", []*slice{sl(0, 1, 4096), sl(0, 1, 4096)}, []bool{true, false}},
		{"disjoint", []*slice{sl(0, 1, 4096), sl(8192, 2, 4096)}, nil},
	}
	for _, c := range cases {
		if got := ploriShadowed(c.ss); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// Design test (3): a file rewritten with O_TRUNC keeps only its last version
// counted once the released slices leave the trash hold (trash on) or at once
// (trash off). The first version spans two chunks, so the shrink releases a
// range of chunks.
func TestDataSpaceTruncRewriteLoop(t *testing.T) {
	versions := [][2]uint32{{64 * dsMiB, 8 * dsMiB}, {3 * dsMiB, 0}, {20 * dsMiB, 0}, {5 * dsMiB, 0}}
	for _, trashDays := range []int{1, 0} {
		t.Run(fmt.Sprintf("trash days %d", trashDays), func(t *testing.T) {
			m, _ := dsOpen(t, 0, 0, trashDays)
			dsObjectsNoop(m)
			f := dsFile(t, m, RootInode, "f")
			var held, last int64
			for i, v := range versions {
				if i > 0 {
					if st := m.Truncate(Background(), f, 0, 0, &Attr{}, false); st != 0 {
						t.Fatalf("truncate: %s", st)
					}
					want := int64(-1)
					if trashDays == 0 {
						want = 0
					}
					dsCheck(t, m, want, fmt.Sprintf("version %d truncated", i))
				}
				dsMustWrite(t, m, f, 0, v[0])
				if v[1] > 0 {
					dsMustWrite(t, m, f, ChunkSize, v[1])
				}
				last = int64(v[0]) + int64(v[1])
				held += last
				want := last
				if trashDays > 0 {
					want = held
				}
				dsCheck(t, m, want, fmt.Sprintf("version %d written", i))
			}
			dsCleanupDelayed(t, m)
			dsCheck(t, m, last, "after the delayed-slice cleanup")
			layout := dsLayout(t, m, f, 0)
			ids := map[uint64]bool{}
			var visible uint32
			for _, s := range layout {
				if s.Id > 0 {
					ids[s.Id] = true
					visible += s.Len
				}
			}
			if len(ids) != 1 || int64(visible) != last {
				t.Fatalf("chunk 0 shows %d bytes of %d slices, want %d bytes of the last version", visible, len(ids), last)
			}
			for _, s := range dsLayout(t, m, f, 1) {
				if s.Id != 0 {
					t.Fatalf("chunk 1 still shows slice %d of the first version", s.Id)
				}
			}
		})
	}
}

// Design test (4): near the ceiling the compaction claim is refused, no
// object is written and the chunk keeps its list and its content; with room
// the compaction runs and its new slice is counted.
func TestSliceDataCompactionRefusedNearCeiling(t *testing.T) {
	const small = 256 << 10
	m, _ := dsOpen(t, 6*dsMiB, 1<<20, 1)
	dsObjectsNoop(m)
	var objects atomic.Int32
	m.OnMsg(CompactChunk, func(args ...interface{}) error {
		objects.Add(1)
		return nil
	})
	f := dsFile(t, m, RootInode, "f")
	for i := 0; i < 16; i++ {
		dsMustWrite(t, m, f, uint64(i)*small, small)
	}
	dsCheck(t, m, 4*dsMiB, "sixteen slices")
	list := dsListBytes(dsList(t, m, f, 0))
	layout := dsLayout(t, m, f, 0)
	k := ploriChunkKey(f, 0)

	m.compactChunk(f, 0, false, true, 0)
	if n := objects.Load(); n != 0 {
		t.Fatalf("a refused compaction wrote %d objects", n)
	}
	if got := dsListBytes(dsList(t, m, f, 0)); !bytes.Equal(got, list) {
		t.Fatal("a refused compaction changed the chunk list")
	}
	if got := dsLayout(t, m, f, 0); !reflect.DeepEqual(got, layout) {
		t.Fatal("a refused compaction changed what reads see")
	}
	dsCheck(t, m, 4*dsMiB, "compaction refused")
	if s, i := volresPending(m.baseMeta); s != 0 || i != 0 {
		t.Fatalf("pending %d/%d after the refusal", s, i)
	}
	m.Lock()
	logged := m.ploriCompactRefused[k]
	m.Unlock()
	if !logged {
		t.Fatal("the refusal was not recorded as logged")
	}

	volresSetCapacity(m.baseMeta, 16*dsMiB)
	m.compactChunk(f, 0, false, true, 0)
	if n := objects.Load(); n != 1 {
		t.Fatalf("compaction wrote %d objects, want 1", n)
	}
	if ss := dsList(t, m, f, 0); len(ss) != 1 || ss[0].len != 4*dsMiB {
		t.Fatalf("compacted chunk has %d slices", len(ss))
	}
	// Trash on: the sixteen slices are held in delslices next to the new one.
	dsCheck(t, m, 8*dsMiB, "compacted")
	if s, i := volresPending(m.baseMeta); s != 0 || i != 0 {
		t.Fatalf("pending %d/%d after the compaction", s, i)
	}
	m.Lock()
	logged = m.ploriCompactRefused[k]
	m.Unlock()
	if logged {
		t.Fatal("a successful claim did not forget the logged refusal")
	}
	dsCleanupDelayed(t, m)
	dsCheck(t, m, 4*dsMiB, "held slices cleaned up")
}

// Logical mode schedules no release and claims nothing for compaction.
func TestSliceDataReleaseLogicalModeUnchanged(t *testing.T) {
	const small = 256 << 10
	lm, err := newSQLMeta("sqlite3", filepath.Join(t.TempDir(), "logical.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lm.Shutdown() })
	format := testFormat()
	format.Capacity = 4 * dsMiB
	format.TrashDays = 1
	if err := lm.Init(format, true); err != nil {
		t.Fatal(err)
	}
	l := lm.(*dbMeta)
	l.refreshUsage()
	dsObjectsNoop(l)
	f := dsFile(t, l, RootInode, "f")
	for i := 0; i < 16; i++ {
		dsMustWrite(t, l, f, uint64(i)*small, small)
	}
	// An overwrite that covers a whole slice: the chunk list keeps it.
	dsMustWrite(t, l, f, 0, dsMiB)
	if n := l.ploriReleasing.Load(); n != 0 {
		t.Fatalf("%d releases scheduled in logical mode", n)
	}
	if ss := dsList(t, l, f, 0); len(ss) != 17 {
		t.Fatalf("logical chunk list has %d entries, want 17", len(ss))
	}
	// At the logical ceiling compaction still runs: it adds no logical space.
	l.compactChunk(f, 0, false, true, 0)
	if ss := dsList(t, l, f, 0); len(ss) != 1 {
		t.Fatalf("logical compaction left %d entries", len(ss))
	}
	// A shrink appends a zero slice and releases nothing.
	if st := l.Truncate(Background(), f, 0, 0, &Attr{}, false); st != 0 {
		t.Fatal(st)
	}
	if n := l.ploriReleasing.Load(); n != 0 {
		t.Fatalf("%d releases scheduled in logical mode", n)
	}
	if ss := dsList(t, l, f, 0); len(ss) != 2 {
		t.Fatalf("logical chunk list has %d entries after the shrink, want 2", len(ss))
	}
	if s, i := volresPending(l.baseMeta); s != 0 || i != 0 {
		t.Fatalf("pending %d/%d in logical mode", s, i)
	}
	if _, ok := dsRow(t, l); ok {
		t.Fatal("logical mode created the ploriDataSpace row")
	}
}

// A release whose chunk changed after its read: an appended slice is kept,
// any other change refuses the commit, and the release rereads.
func TestSliceDataReleaseOriginCheck(t *testing.T) {
	m, _ := dsOpen(t, 0, 0, 1)
	dsObjectsNoop(m)
	f := dsFile(t, m, RootInode, "f")
	k := ploriChunkKey(f, 0)
	// Hold the chunk as a compaction would, so the releases the writes
	// schedule are only recorded.
	m.Lock()
	m.compacting[k] = true
	m.Unlock()
	a, _ := dsWriteID(t, m, Background(), f, 0, dsMiB)
	b, _ := dsWriteID(t, m, Background(), f, 0, dsMiB)
	dsDrain(t, m)
	m.Lock()
	requested := m.ploriReleaseAgain[k]
	m.Unlock()
	if !requested {
		t.Fatal("a release requested while the chunk was held was not recorded")
	}
	ss := dsList(t, m, f, 0)
	drop := ploriShadowed(ss)
	if !reflect.DeepEqual(drop, []bool{true, false}) || ss[0].id != a || ss[1].id != b {
		t.Fatalf("setup: list %v drop %v", ss, drop)
	}
	c, _ := dsWriteID(t, m, Background(), f, 2*dsMiB, dsMiB)
	if st := m.ploriReleaseCommit(f, 0, ss, drop); st != 0 {
		t.Fatalf("commit after an append: %s", st)
	}
	if got := dsList(t, m, f, 0); len(got) != 2 || got[0].id != b || got[1].id != c {
		t.Fatalf("after the commit the list is %v, want slices %d and %d", got, b, c)
	}
	if st := m.ploriReleaseCommit(f, 0, ss, drop); st != syscall.EINVAL {
		t.Fatalf("commit with a stale list = %s, want EINVAL", st)
	}
	if st := m.doPloriReleaseShadowed(f, 0); st != 0 {
		t.Fatal(st)
	}
	if got := dsList(t, m, f, 0); len(got) != 2 {
		t.Fatalf("a reread release changed the list to %v", got)
	}
	m.Lock()
	delete(m.compacting, k)
	delete(m.ploriReleaseAgain, k)
	m.Unlock()
	dsCheck(t, m, 3*dsMiB, "a held in delslices")
	dsCleanupDelayed(t, m)
	dsCheck(t, m, 2*dsMiB, "a cleaned up")
}

// A release requested while a compaction holds the chunk runs when the
// compaction finishes, and drops the compacted slice a later write covered.
func TestSliceDataReleaseAfterCompaction(t *testing.T) {
	const small = 256 << 10
	m, _ := dsOpen(t, 0, 0, 1)
	dsObjectsNoop(m)
	entered, proceed := make(chan struct{}), make(chan struct{})
	m.OnMsg(CompactChunk, func(args ...interface{}) error {
		close(entered)
		<-proceed
		return nil
	})
	f := dsFile(t, m, RootInode, "f")
	for i := 0; i < 16; i++ {
		dsMustWrite(t, m, f, uint64(i)*small, small)
	}
	done := make(chan struct{})
	go func() {
		m.compactChunk(f, 0, false, false, 0)
		close(done)
	}()
	<-entered
	x, _ := dsWriteID(t, m, Background(), f, 0, 4*dsMiB)
	for m.ploriReleasing.Load() > 0 {
		time.Sleep(time.Millisecond)
	}
	m.Lock()
	requested := m.ploriReleaseAgain[ploriChunkKey(f, 0)]
	m.Unlock()
	if !requested {
		t.Fatal("the release was not handed to the compaction")
	}
	close(proceed)
	<-done
	dsDrain(t, m)
	if ss := dsList(t, m, f, 0); len(ss) != 1 || ss[0].id != x {
		t.Fatalf("after the compaction and the release the list is %v, want only slice %d", ss, x)
	}
	// Held: the sixteen compacted slices and the compacted slice itself.
	dsCheck(t, m, 12*dsMiB, "compacted and released")
	dsCleanupDelayed(t, m)
	dsCheck(t, m, 4*dsMiB, "held slices cleaned up")
}

// Race: releases run against a stream of overlapping writes on one chunk.
// Reads see the same content as without any release, no visible slice loses
// its reference, and the counters agree.
func TestSliceDataReleaseRacesWrites(t *testing.T) {
	for _, trashDays := range []int{0, 1} {
		t.Run(fmt.Sprintf("trash days %d", trashDays), func(t *testing.T) {
			seed := time.Now().UnixNano()
			t.Logf("seed %d", seed)
			rnd := rand.New(rand.NewSource(seed))
			m, _ := dsOpen(t, 0, 0, trashDays)
			dsObjectsNoop(m)
			f := dsFile(t, m, RootInode, "f")
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						if st := m.doPloriReleaseShadowed(f, 0); st != 0 {
							t.Errorf("release: %s", st)
							return
						}
					}
				}()
			}
			var model []*slice
			var written int64
			for i := 0; i < 200; i++ {
				off := uint32(rnd.Intn(128)) << 12
				size := uint32(1+rnd.Intn(64)) << 12
				id, st := dsWriteID(t, m, Background(), f, uint64(off), size)
				if st != 0 {
					t.Fatalf("write: %s", st)
				}
				model = append(model, &slice{pos: off, id: id, size: size, len: size})
				written += int64(size)
			}
			close(stop)
			wg.Wait()
			dsDrain(t, m)
			ss := dsList(t, m, f, 0)
			if got, want := dsZeros(buildSlice(ss)), dsZeros(buildSlice(model)); !reflect.DeepEqual(got, want) {
				t.Fatalf("reads differ from the written sequence:\n%v\n%v", got, want)
			}
			if len(ss) >= len(model) {
				t.Fatalf("no slice was released: %d of %d entries left", len(ss), len(model))
			}
			var listed int64
			for _, s := range ss {
				ref := sliceRef{Id: s.id}
				var ok bool
				if err := m.simpleTxn(Background(), func(se *xorm.Session) error {
					var e error
					ok, e = se.Get(&ref)
					return e
				}); err != nil || !ok || ref.Refs <= 0 {
					t.Fatalf("listed slice %d: refs %d found %t err %v", s.id, ref.Refs, ok, err)
				}
				listed += int64(s.size)
			}
			want := listed
			if trashDays > 0 {
				want = written
			}
			dsCheck(t, m, want, "after the race")
		})
	}
}

// Race: a compaction claim and a foreground write compete for the last free
// bytes. Exactly one of them fits and runs, and the slice data never exceeds
// the capacity.
func TestSliceDataCompactionClaimRacesWrite(t *testing.T) {
	const small = 256 << 10
	for round := 0; round < 8; round++ {
		// 4 MiB in f, 1 MiB in g, capacity 11 MiB: 6 MiB free, and the
		// compaction and the write need 4 MiB each.
		m, _ := dsOpen(t, 11*dsMiB, 1<<20, 1)
		dsObjectsNoop(m)
		f := dsFile(t, m, RootInode, "f")
		for i := 0; i < 16; i++ {
			dsMustWrite(t, m, f, uint64(i)*small, small)
		}
		dsMustWrite(t, m, dsFile(t, m, RootInode, "g"), 0, dsMiB)
		h := dsFile(t, m, RootInode, "h")
		start := make(chan struct{})
		var wg sync.WaitGroup
		var writeSt syscall.Errno
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			m.compactChunk(f, 0, false, true, 0)
		}()
		go func() {
			defer wg.Done()
			ctx := volresCtx(true)
			<-start
			_, writeSt = dsWriteID(t, m, ctx, h, 0, 4*dsMiB)
			volresRelease(m.baseMeta, ctx)
		}()
		close(start)
		wg.Wait()
		compacted := len(dsList(t, m, f, 0)) == 1
		wrote := writeSt == 0
		if writeSt != 0 && writeSt != syscall.ENOSPC {
			t.Fatalf("round %d: write: %s", round, writeSt)
		}
		if compacted == wrote {
			t.Fatalf("round %d: compacted %t, write admitted %t; exactly one must fit", round, compacted, wrote)
		}
		if used := dsCheck(t, m, 9*dsMiB, fmt.Sprintf("round %d", round)); used > 11*dsMiB {
			t.Fatalf("round %d: slice data %d exceeds the capacity", round, used)
		}
		if s, i := volresPending(m.baseMeta); s != 0 || i != 0 {
			t.Fatalf("round %d: pending %d/%d", round, s, i)
		}
	}
}
