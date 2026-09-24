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
	"syscall"
)

// This file carries NO `plori` build tag, and that is deliberate: it is the one
// trash-usage walk, and it has two callers on two different builds.
//
//   - The live mount, built with the release tag set, on its lease-renew tick
//     (cmd/plori_mount.go, ploriVolume.Usage).
//   - plori-runtime's services/storage-worker, which answers the same question
//     for an Agent that is ASLEEP and therefore has no mount. That module links
//     this fork as a library on a PLAIN build — no tags at all — so anything
//     behind `//go:build plori` is invisible to it.
//
// It was tagged until PLO-429, and the cost was a second copy of the arithmetic
// in the worker (services/storage-worker/internal/jfsvol/usage.go), which is
// exactly the drift the trash-is-a-subset-of-used_bytes claim cannot survive:
// the two ends would disagree about the same volume.
//
// Untagging is the fix rather than a sibling package under pkg/plori (the
// mountspec pattern) because of what the walk is made of. mountspec is plain
// wire data and imports nothing from this package; this walk is the metadata
// engine's own accounting and needs align4K (utils.go) and recordStat's rules.
// A package outside pkg/meta cannot reach align4K, so it would have to restate
// it — the same duplication, one level down — and pkg/meta could not then alias
// back into it without an import cycle. The counting rule belongs in the
// package that owns the counter.
//
// Nothing here runs unless it is called, so a vanilla build carries the code and
// no behaviour. The guard that keeps it reachable is plori_trash_test.go, which
// is untagged for the same reason and is compiled on the default build by
// `make test.plori.sqlite`; the cross-repo half is plori-runtime's
// services/storage-worker/internal/mountwire trash-walk parity test.

// PloriTrashDirName is Plori's OWN soft-delete namespace, a plain directory at the
// volume root. The Files panel renames a deleted file into it under CAS and hands the
// new path back as the undo handle; a background sweep hard-deletes entries older than
// the panel's TTL, and that hard delete is what finally moves the bytes into the
// JuiceFS trash below.
//
// It exists because a JuiceFS trash entry is named `<parent inode>-<inode>-<name>` and
// therefore does not carry the original DIRECTORY, so "restore to where it was" needs
// an index of its own. Collapsing the two is PLO-399, at Orlop retirement.
const PloriTrashDirName = ".plori-trash"

// PloriDefaultTrashWalkCap bounds the walk below. It is an ENTRY budget, not a depth
// or a time limit, because the only unbounded dimension here is how many files one
// Agent has deleted.
//
// 200 000 is the same order as the Files panel's own recursive-read ceiling
// (control-plane services_bundle.go caps a bundle at 200 000 files) and, at the 20 s
// renew interval this runs on, a walk that size costs a few tens of milliseconds of
// local SQLite reads. Above it the report says `partial` and the product omits the
// number rather than showing a floor as if it were the answer.
const PloriDefaultTrashWalkCap = 200_000

// PloriTrashUsage is what the two trash namespaces of one volume are holding.
//
// Bytes and Inodes are counted the way the volume's own `used_bytes` counts them —
// `align4K(length)` per file, one 4 KiB block per directory, hard links counted once —
// so the number is always a SUBSET of `used_bytes` and never something to add to it.
// See PloriMeasureTrash for why that is true rather than hoped for.
//
// PloriMeasureTrashSliceData reports Bytes as the slice data that emptying the trash
// would release (ploriTrashReclaimable), which is a subset of the slice_data figure. Inodes is counted the same way in both bases, because
// the inode ceiling stays per copy.
type PloriTrashUsage struct {
	Bytes  int64
	Inodes int64
	// Partial is true when the walk hit its entry budget. The numbers are then a
	// floor, and the caller must not present them as an amount.
	Partial bool
}

// PloriMeasureTrash sums both trash namespaces of the volume `m` is opened on.
//
// # Why a volume's used_bytes ALREADY contains this
//
// With `TrashDays > 0` the metadata engine turns every unlink into a rename into
// `.trash/<YYYY-MM-DD-HH>/` (checkTrash / doUnlink), so no `updateStats` with a
// negative delta ever runs: the file's `align4K(length)` stays inside the volume's
// `usedSpace` counter, which is what StatFS reports and what the volume ceiling is
// enforced against. Creating a new hour bucket ADDS `updateStats(align4K(0), 1)`
// (base.go checkTrash), which is why PLO-335 measured a delete moving StatFS from
// 20480 B / 5 inodes to 24576 B / 6 rather than releasing anything.
//
// The authority for the arithmetic below is JuiceFS's own recomputation of the
// counter: `fsck --repair --sync-dir-stat` walks `/` and then `/.trash` into the SAME
// `volumeUsed`/`volumeInodes` accumulators, adding `align4K(attr.Length)` per file and
// `align4K(0)` per directory, de-duplicating hard links, and skipping exactly two
// inodes — RootInode and TrashInode (base.go recordStat). This function is that
// accumulator restricted to the trash subtrees, which is what makes its result a
// subset of `used_bytes` by construction and not by coincidence.
//
// The `.trash` root itself is therefore NOT counted: it is created by Init without an
// `updateStats` call and skipped by recordStat, so counting it here would make the
// breakdown exceed the whole. `/.plori-trash` IS counted, root directory included: it
// is an ordinary directory that an ordinary `mkdir` created, and its 4 KiB is inside
// `used_bytes` like any other directory's.
//
// # Why it walks instead of asking for a summary
//
// `GetSummary(TrashInode, recursive, strict=false)` is the cheaper call and it is what
// `juicefs summary` runs, but it reads per-directory statistics, and a missing record
// makes `doGetDirStat` SYNC one — a metadata write. This runs on a read-only replica
// session in the storage worker and on the single writer in the mount, so a read that
// can write is not something either caller can afford. `strict=true` avoids the write
// but has no budget: it cannot stop. This walk reads the same `Readdir(plus=1)` pages
// `strict=true` reads, and stops.
//
// # Permissions
//
// `.trash` is mode 0555 owned by uid 0 and its entries are not reachable from inside
// the mount at all (base.go refuses Lookup of `.trash` at the root and every mutation
// under it for a non-zero uid). `ctx` must therefore be a uid-0 context — both callers
// are the trusted worker process, never the Agent.
//
// An absent namespace is zero, not an error: an Agent that has never deleted anything
// has neither directory. Any other failure is returned, and the caller reports
// `used_bytes` with no breakdown rather than a guess.
func PloriMeasureTrash(m Meta, ctx Context, entryCap int) (PloriTrashUsage, error) {
	return ploriMeasureTrash(m, ctx, entryCap, false)
}

// PloriMeasureTrashSliceData is PloriMeasureTrash for a volume whose used bytes are
// counted on the slice_data basis (meta.PloriQuotaBasis). Inodes and Partial are
// counted as PloriMeasureTrash counts them; Bytes is not.
//
// On that basis `used_bytes` is the sum of the sizes of the slices that still have a
// reference, and a trash file's length says nothing about what purging it releases: a
// clone shares its slices with the source, so a 1 GB revision in the trash can release
// a few MB. Bytes is the sum of the sizes of the slices whose every reference comes
// from a trash file (ploriTrashReclaimable). Each such slice is inside `used_bytes`
// once, so the result is a subset of it by the same construction. A partial walk sees
// fewer trash references, so fewer slices qualify and the number stays a floor.
//
// The caller names the basis rather than this function reading it, because the basis
// is set by the plori-mount build only and this file is also linked on the plain build.
// Only the SQL engine keeps the slice references this needs; any other engine is an
// error, and the caller reports `used_bytes` with no breakdown.
func PloriMeasureTrashSliceData(m Meta, ctx Context, entryCap int) (PloriTrashUsage, error) {
	return ploriMeasureTrash(m, ctx, entryCap, true)
}

func ploriMeasureTrash(m Meta, ctx Context, entryCap int, sliceData bool) (PloriTrashUsage, error) {
	if entryCap <= 0 {
		entryCap = PloriDefaultTrashWalkCap
	}
	w := ploriTrashWalk{budget: entryCap, links: make(map[Ino]uint32)}
	var reader ploriTrashSliceReader
	if sliceData {
		var ok bool
		if reader, ok = m.getBase().en.(ploriTrashSliceReader); !ok {
			return PloriTrashUsage{}, fmt.Errorf("the %s metadata engine keeps no slice references to measure the trash with", m.Name())
		}
		w.files = make(map[Ino]uint32)
	}

	// JuiceFS's own trash. The root is excluded from the volume counter, so it is
	// excluded here; its hour buckets and their contents are not.
	if st := w.walk(m, ctx, TrashInode); st != 0 && st != syscall.ENOENT {
		return PloriTrashUsage{}, fmt.Errorf("walk %s: %w", TrashName, st)
	}

	// Plori's undo index. An ordinary directory, so its own block counts too.
	var ino Ino
	var attr Attr
	switch st := m.Lookup(ctx, RootInode, PloriTrashDirName, &ino, &attr, false); st {
	case 0:
		w.u.Bytes += align4K(0)
		w.u.Inodes++
		if st := w.walk(m, ctx, ino); st != 0 && st != syscall.ENOENT {
			return PloriTrashUsage{}, fmt.Errorf("walk /%s: %w", PloriTrashDirName, st)
		}
	case syscall.ENOENT:
	default:
		return PloriTrashUsage{}, fmt.Errorf("lookup /%s: %w", PloriTrashDirName, st)
	}
	if reader == nil {
		return w.u, nil
	}
	bytes, err := reader.ploriTrashReclaimable(ctx, w.releasedByPurge())
	if err != nil {
		return PloriTrashUsage{}, fmt.Errorf("read the slice references of the trash: %w", err)
	}
	w.u.Bytes = bytes
	return w.u, nil
}

// ploriTrashSliceReader is implemented by the SQL engine (plori_trash_sql.go), the
// only engine that keeps the slice data counter.
type ploriTrashSliceReader interface {
	// ploriTrashReclaimable reads the chunk lists of `files` and the chunk_ref rows of
	// every slice they name in one read-only transaction, and returns the summed size
	// of the slices whose refs all come from those chunk lists.
	ploriTrashReclaimable(ctx Context, files []Ino) (int64, error)
}

// ploriTrashWalk is the state shared by the walks of both trash namespaces.
type ploriTrashWalk struct {
	budget int
	// links counts the trash names seen for each inode with Nlink > 1.
	links map[Ino]uint32
	// files maps every regular file seen in the trash to its Nlink. It is nil unless
	// the volume counts on the slice_data basis.
	files map[Ino]uint32
	u     PloriTrashUsage
}

// releasedByPurge returns the trash files whose data a purge of the trash releases:
// those with every hard link inside the trash. A file with a name outside the trash
// keeps its chunk lists after the purge, so its slices are not released.
func (w *ploriTrashWalk) releasedByPurge() []Ino {
	files := make([]Ino, 0, len(w.files))
	for ino, nlink := range w.files {
		if nlink <= 1 || w.links[ino] >= nlink {
			files = append(files, ino)
		}
	}
	return files
}

// walk accumulates every entry BELOW `root`, iteratively so a deleted directory tree
// cannot recurse the stack away, and stops when the budget runs out.
func (w *ploriTrashWalk) walk(m Meta, ctx Context, root Ino) syscall.Errno {
	u := &w.u
	stack := []Ino{root}
	for len(stack) > 0 {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		var entries []*Entry
		if st := m.Readdir(ctx, dir, 1, &entries); st != 0 {
			// A bucket the purger removed between the readdir above and this one is
			// gone, not a failure: it is trash that is no longer holding anything.
			if st == syscall.ENOENT && dir != root {
				continue
			}
			return st
		}
		for _, e := range entries {
			if len(e.Name) == 1 && e.Name[0] == '.' {
				continue
			}
			if len(e.Name) == 2 && e.Name[0] == '.' && e.Name[1] == '.' {
				continue
			}
			if w.budget <= 0 {
				u.Partial = true
				return 0
			}
			w.budget--
			if e.Attr == nil {
				continue
			}
			if e.Attr.Typ == TypeDirectory {
				u.Bytes += align4K(0)
				u.Inodes++
				stack = append(stack, e.Inode)
				continue
			}
			// Hard links occupy their blocks once. recordStat de-duplicates the same
			// way, and a trash full of links to one file would otherwise report space
			// that emptying it would not free.
			if e.Attr.Nlink > 1 {
				w.links[e.Inode]++
				if w.links[e.Inode] > 1 {
					continue
				}
			}
			u.Bytes += align4K(e.Attr.Length)
			u.Inodes++
			if w.files != nil && e.Attr.Typ == TypeFile {
				w.files[e.Inode] = e.Attr.Nlink
			}
		}
	}
	return 0
}
