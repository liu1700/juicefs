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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

type cloneHLGroup struct {
	source Ino
	paths  []string
}

func cloneHLPath(t *testing.T, m Meta, root Ino, path string) Ino {
	t.Helper()
	for _, name := range strings.Split(path, "/") {
		root = dsLookup(t, m, root, name)
	}
	return root
}

func cloneHLTree(t *testing.T, m Meta) (Ino, []cloneHLGroup) {
	t.Helper()
	src := dsDir(t, m, RootInode, "src")
	x := dsDir(t, m, src, "x")
	dsDir(t, m, src, "y")
	dsDir(t, m, x, "z")
	groups := []cloneHLGroup{
		{paths: []string{"two", "two-again"}},
		{paths: []string{"three", "x/z/three", "y/three"}},
		{paths: []string{"x/outside", "y/outside"}},
		{paths: []string{"only-inside"}},
	}
	for i := range groups {
		g := &groups[i]
		for j, path := range g.paths {
			parent := src
			if dir := filepath.Dir(path); dir != "." {
				parent = cloneHLPath(t, m, src, dir)
			}
			if j == 0 {
				g.source = dsFile(t, m, parent, filepath.Base(path))
				dsMustWrite(t, m, g.source, 0, 4096)
				if st := m.SetXattr(Background(), g.source, "user.clone", []byte("preserved"), 0); st != 0 {
					t.Fatal(st)
				}
			} else if st := m.Link(Background(), g.source, parent, filepath.Base(path), nil); st != 0 {
				t.Fatal(st)
			}
		}
		if i >= 2 {
			if st := m.Link(Background(), g.source, RootInode, fmt.Sprintf("outside-%d", i), nil); st != 0 {
				t.Fatal(st)
			}
		}
	}
	m.getBase().doFlushDirStat()
	return src, groups
}

func cloneHLRead(t *testing.T, m Meta, ino Ino) []Slice {
	t.Helper()
	var slices []Slice
	if st := m.Read(Background(), ino, 0, &slices); st != 0 {
		t.Fatal(st)
	}
	return slices
}

func cloneHLVerify(t *testing.T, m Meta, dst Ino, groups []cloneHLGroup) {
	t.Helper()
	for _, g := range groups {
		first := cloneHLPath(t, m, dst, g.paths[0])
		if first == g.source {
			t.Fatal("clone reused source inode")
		}
		for _, path := range g.paths {
			ino := cloneHLPath(t, m, dst, path)
			if ino != first {
				t.Fatalf("%s inode %d != %d", path, ino, first)
			}
			var attr Attr
			if st := m.GetAttr(Background(), ino, &attr); st != 0 {
				t.Fatal(st)
			}
			if attr.Nlink != uint32(len(g.paths)) {
				t.Fatalf("%s nlink %d != %d", path, attr.Nlink, len(g.paths))
			}
			if (len(g.paths) > 1) != (attr.Parent == 0) {
				t.Fatalf("%s parent %d", path, attr.Parent)
			}
			if got, want := cloneHLRead(t, m, ino), cloneHLRead(t, m, g.source); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s slices %v != %v", path, got, want)
			}
			var value []byte
			if st := m.GetXattr(Background(), ino, "user.clone", &value); st != 0 || string(value) != "preserved" {
				t.Fatalf("xattr %s: %q %s", path, value, st)
			}
		}
	}
	if err := m.Check(Background(), "/dst", &CheckOpt{Recursive: true, SyncDirStat: true, ShowProgress: func(int) {}, Slices: make(map[Ino][]Slice)}); err != nil {
		t.Fatal(err)
	}
}

func cloneHLRefs(t *testing.T, m *dbMeta, groups []cloneHLGroup, want int) {
	t.Helper()
	for _, g := range groups {
		slices := cloneHLRead(t, m, g.source)
		for _, slice := range slices {
			r := sliceRef{Id: slice.Id, Size: slice.Size}
			if ok, err := m.db.Get(&r); err != nil || !ok || r.Refs != want {
				t.Fatalf("slice %d refs %d want %d: exists=%t err=%v", slice.Id, r.Refs, want, ok, err)
			}
		}
	}
}

func TestCloneHardlinksSQLite(t *testing.T) {
	for _, data := range []bool{false, true} {
		for _, reserved := range []bool{false, true} {
			for _, concurrency := range []uint8{1, 8} {
				t.Run(fmt.Sprintf("slice_data=%t/reserved=%t/concurrency=%d", data, reserved, concurrency), func(t *testing.T) {
					mm, b := volresOpen(t, volresSQLiteEngine, 1<<30, 0)
					m := mm.(*dbMeta)
					if data {
						if _, _, _, err := m.ploriEnableSliceData(); err != nil {
							t.Fatal(err)
						}
						b.refreshUsage()
					}
					src, groups := cloneHLTree(t, m)
					beforeSpace, beforeInodes := b.volumeClaimed()
					logicalBefore := atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace)
					// Only eight distinct inodes fit. Counting twelve paths must refuse.
					f := *b.getFormat()
					f.Inodes = uint64(beforeInodes + 8)
					f.Capacity = uint64(beforeSpace)
					if !data {
						f.Capacity += 8 * 4096
					}
					f.UserGroupQuota = true
					b.setFormat(&f)
					uq := &Quota{MaxSpace: 8 * 4096, MaxInodes: 8}
					gq := &Quota{MaxSpace: 8 * 4096, MaxInodes: 8}
					dq := &Quota{MaxSpace: 12 * 4096, MaxInodes: 12}
					b.userQuotas[0], b.groupQuotas[0], b.dirQuotas[uint64(RootInode)] = uq, gq, dq
					ctx := volresCtx(reserved)
					defer volresRelease(b, ctx)
					var count, total uint64
					if st := m.Clone(ctx, RootInode, src, RootInode, "dst", CLONE_MODE_PRESERVE_ATTR, 0, concurrency, &count, &total); st != 0 {
						t.Fatal(st)
					}
					if count != 12 || total != 12 {
						t.Fatalf("progress %d/%d want 12/12", count, total)
					}
					if s, i := volresPending(b); s != 0 || i != 0 {
						t.Fatalf("pending %d/%d", s, i)
					}
					afterSpace, afterInodes := b.volumeClaimed()
					wantSpace := int64(8 * 4096)
					if data {
						wantSpace = 0
						dsCheck(t, m, 4*4096, "clone")
					}
					if afterSpace-beforeSpace != wantSpace || afterInodes-beforeInodes != 8 {
						t.Fatalf("volume delta %d/%d want %d/8", afterSpace-beforeSpace, afterInodes-beforeInodes, wantSpace)
					}
					if got := atomic.LoadInt64(&b.usedSpace) + atomic.LoadInt64(&b.newSpace) - logicalBefore; got != 8*4096 {
						t.Fatalf("logical usage %d", got)
					}
					for name, q := range map[string]*Quota{"user": uq, "group": gq, "directory": dq} {
						want := int64(8)
						if name == "directory" {
							want = 12
						}
						if q.newSpace != want*4096 || q.newInodes != want {
							t.Fatalf("%s quota %+v want %d entries", name, q, want)
						}
					}
					dst := dsLookup(t, m, RootInode, "dst")
					cloneHLVerify(t, m, dst, groups)
					cloneHLRefs(t, m, groups, 2)
					for _, path := range []string{".", "x", "x/z", "y"} {
						sourceDir, destDir := src, dst
						if path != "." {
							sourceDir = cloneHLPath(t, m, src, path)
							destDir = cloneHLPath(t, m, dst, path)
						}
						a, st := m.GetDirStat(Background(), sourceDir)
						if st != 0 {
							t.Fatal(st)
						}
						b, st := m.GetDirStat(Background(), destDir)
						if st != 0 {
							t.Fatal(st)
						}
						if *a != *b {
							t.Fatalf("dir stat %s: source %+v clone %+v", path, a, b)
						}
					}
					// Relax ceilings before writing a new physical slice.
					f.Capacity, f.Inodes = 1<<30, 1<<20
					b.setFormat(&f)
					for _, q := range []*Quota{uq, gq, dq} {
						q.MaxSpace, q.MaxInodes = -1, -1
					}
					first := cloneHLPath(t, m, dst, "two")
					old := cloneHLRead(t, m, groups[0].source)
					dsMustWrite(t, m, first, 0, 4096)
					got := cloneHLRead(t, m, first)
					if reflect.DeepEqual(got, old) {
						t.Fatal("write did not change cloned slices")
					}
					if !reflect.DeepEqual(got, cloneHLRead(t, m, cloneHLPath(t, m, dst, "two-again"))) {
						t.Fatal("sibling did not see write")
					}
					if !reflect.DeepEqual(old, cloneHLRead(t, m, groups[0].source)) {
						t.Fatal("source changed")
					}
					if st := m.Unlink(Background(), dst, "two", true); st != 0 {
						t.Fatal(st)
					}
					sibling := cloneHLPath(t, m, dst, "two-again")
					var attr Attr
					if st := m.GetAttr(Background(), sibling, &attr); st != 0 || attr.Nlink != 1 {
						t.Fatalf("unlink sibling: %+v %s", attr, st)
					}
					if !reflect.DeepEqual(got, cloneHLRead(t, m, sibling)) {
						t.Fatal("unlink lost data")
					}
					var deleted sync.Map
					m.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted.Store(args[0].(uint64), true); return nil })
					if st := m.Remove(Background(), RootInode, "dst", true, 8, nil); st != 0 {
						t.Fatal(st)
					}
					dsDrain(t, m)
					if _, err := m.doCleanupDelayedSlices(Background(), 1<<62); err != nil {
						t.Fatal(err)
					}
					dsDrain(t, m)
					cloneHLRefs(t, m, groups, 1)
					for _, g := range groups {
						for _, s := range cloneHLRead(t, m, g.source) {
							if _, ok := deleted.Load(s.Id); ok {
								t.Fatalf("GC deleted source slice %d", s.Id)
							}
						}
					}
					if data {
						dsCheck(t, m, 4*4096, "clone removed")
					}
					if q := dq.snap(); q.newSpace != 0 || q.newInodes != 0 {
						t.Fatalf("directory quota after remove %+v", q)
					}
				})
			}
		}
	}
}

func TestCloneHardlinksSQLiteRecovery(t *testing.T) {
	m, path := dsOpen(t, 1<<30, 1<<20, 0)
	src, groups := cloneHLTree(t, m)
	var count, total uint64
	if st := m.Clone(Background(), RootInode, src, RootInode, "dst", CLONE_MODE_PRESERVE_ATTR, 0, 8, &count, &total); st != 0 {
		t.Fatal(st)
	}
	dst := dsLookup(t, m, RootInode, "dst")
	m.doFlushStats()
	m.doFlushDirStat()
	// Quiescent checkpoint + file copy models restoration of a complete SQLite
	// image. No live WAL is omitted and the restored DB has its own pathname.
	if _, err := m.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(restored, content, 0600); err != nil {
		t.Fatal(err)
	}
	r := dsReopen(t, restored)
	if _, _, _, err := r.ploriEnableSliceData(); err != nil {
		t.Fatal(err)
	}
	r.refreshUsage()
	cloneHLVerify(t, r, dst, groups)
	cloneHLRefs(t, r, groups, 2)
	dsCheck(t, r, 4*4096, "restored")
	// A new clone must use a fresh in-memory inode map after restoration.
	if st := volresClone(r, Background(), src, "another"); st != 0 {
		t.Fatal(st)
	}
	other := dsLookup(t, r, RootInode, "another")
	if cloneHLPath(t, r, dst, "two") == cloneHLPath(t, r, other, "two") {
		t.Fatal("separate clones share inode")
	}
}

func TestCloneHardlinksUnsupported(t *testing.T) {
	for _, e := range volresEngines {
		if e.name == "sqlite3" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			m, b := volresOpen(t, e, 1<<30, 0)
			src, groups := cloneHLTree(t, m)
			beforeSpace, beforeInodes := b.volumeClaimed()
			var paths, total uint64
			if st := m.Clone(Background(), RootInode, src, RootInode, "dst", CLONE_MODE_PRESERVE_ATTR, 0, 8, &paths, &total); st != syscall.ENOTSUP {
				t.Fatalf("tree clone: %s", st)
			}
			if space, inodes := b.volumeClaimed(); space != beforeSpace || inodes != beforeInodes {
				t.Fatalf("unsupported preflight changed usage: %d/%d", space, inodes)
			}
			ino, err := b.nextInode()
			if err != nil {
				t.Fatal(err)
			}
			if st := b.en.doCloneEntry(Background(), groups[0].source, RootInode, "direct", ino, &Attr{}, 0, 0, true); st != syscall.ENOTSUP {
				t.Fatalf("engine clone: %s", st)
			}
			if st := volresClone(m, Background(), groups[0].source, "file"); st != syscall.ENOTSUP {
				t.Fatalf("file clone: %s", st)
			}
			dst := dsDir(t, m, RootInode, "batch")
			var count uint64
			entries := []*Entry{{Inode: groups[0].source, Name: []byte("a"), Attr: &Attr{Typ: TypeFile}}}
			if st := b.BatchClone(Background(), src, dst, entries, 0, 0, &count); st != syscall.ENOTSUP {
				t.Fatalf("batch clone: %s", st)
			}
			if st := m.Lookup(Background(), RootInode, "dst", new(Ino), &Attr{}, false); st != syscall.ENOENT {
				t.Fatalf("failed clone published destination: %s", st)
			}
		})
	}
}

// Start all callers together, including calls for the same source inode.
// A failed first insert must not publish an inode to any later caller.
func TestCloneHardlinksConcurrentEntries(t *testing.T) {
	mm, b := volresOpen(t, volresSQLiteEngine, 1<<30, 0)
	m := mm.(*dbMeta)
	src := dsFile(t, m, RootInode, "source")
	dsMustWrite(t, m, src, 0, 4096)
	if st := m.Link(Background(), src, RootInode, "source-again", nil); st != 0 {
		t.Fatal(st)
	}
	dst := dsDir(t, m, RootInode, "dst")
	dsFile(t, m, dst, "occupied")
	links := cloneLinks{src: &cloneLink{}}
	ctx := Background().WithValue(cloneLinksKey{}, links)
	var count uint64
	if st := b.cloneEntry(ctx, src, dst, "occupied", nil, CLONE_MODE_PRESERVE_ATTR, 0, &count, false, nil); st != syscall.EEXIST {
		t.Fatalf("conflict: %s", st)
	}
	if links[src].ino != 0 {
		t.Fatal("failed transaction published an inode")
	}
	beforeSpace, beforeInodes := b.volumeClaimed()
	const names = 32
	start := make(chan struct{})
	results := make(chan syscall.Errno, names)
	var wg sync.WaitGroup
	for i := 0; i < names; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results <- b.cloneEntry(ctx, src, dst, fmt.Sprint(i), nil, CLONE_MODE_PRESERVE_ATTR, 0, &count, false, nil)
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	for st := range results {
		if st != 0 {
			t.Fatal(st)
		}
	}
	first := dsLookup(t, m, dst, "0")
	for i := 0; i < names; i++ {
		if ino := dsLookup(t, m, dst, fmt.Sprint(i)); ino != first {
			t.Fatalf("%d maps to %d want %d", i, ino, first)
		}
	}
	var attr Attr
	if st := m.GetAttr(ctx, first, &attr); st != 0 || attr.Nlink != names || attr.Parent != 0 {
		t.Fatalf("attr %+v: %s", attr, st)
	}
	if st := b.cloneEntry(ctx, src, dst, "0", nil, CLONE_MODE_PRESERVE_ATTR, 0, &count, false, nil); st != syscall.EEXIST {
		t.Fatalf("link conflict: %s", st)
	}
	if st := m.GetAttr(ctx, first, &attr); st != 0 || attr.Nlink != names {
		t.Fatalf("failed link changed nlink: %+v %s", attr, st)
	}
	afterSpace, afterInodes := b.volumeClaimed()
	if count != names || afterSpace-beforeSpace != 4096 || afterInodes-beforeInodes != 1 {
		t.Fatalf("count=%d volume delta=%d/%d", count, afterSpace-beforeSpace, afterInodes-beforeInodes)
	}
	cloneHLRefs(t, m, []cloneHLGroup{{source: src}}, 2)
}
