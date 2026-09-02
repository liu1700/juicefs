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
	"fmt"
	"syscall"
)

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
	if entryCap <= 0 {
		entryCap = PloriDefaultTrashWalkCap
	}
	var u PloriTrashUsage
	budget := entryCap
	seen := make(map[Ino]bool)

	// JuiceFS's own trash. The root is excluded from the volume counter, so it is
	// excluded here; its hour buckets and their contents are not.
	if st := ploriWalkTrash(m, ctx, TrashInode, &budget, seen, &u); st != 0 && st != syscall.ENOENT {
		return PloriTrashUsage{}, fmt.Errorf("walk %s: %w", TrashName, st)
	}

	// Plori's undo index. An ordinary directory, so its own block counts too.
	var ino Ino
	var attr Attr
	switch st := m.Lookup(ctx, RootInode, PloriTrashDirName, &ino, &attr, false); st {
	case 0:
		u.Bytes += align4K(0)
		u.Inodes++
		if st := ploriWalkTrash(m, ctx, ino, &budget, seen, &u); st != 0 && st != syscall.ENOENT {
			return PloriTrashUsage{}, fmt.Errorf("walk /%s: %w", PloriTrashDirName, st)
		}
	case syscall.ENOENT:
	default:
		return PloriTrashUsage{}, fmt.Errorf("lookup /%s: %w", PloriTrashDirName, st)
	}
	return u, nil
}

// ploriWalkTrash accumulates every entry BELOW `root`, iteratively so a deleted
// directory tree cannot recurse the stack away, and stops when the budget runs out.
func ploriWalkTrash(m Meta, ctx Context, root Ino, budget *int, seen map[Ino]bool, u *PloriTrashUsage) syscall.Errno {
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
			if *budget <= 0 {
				u.Partial = true
				return 0
			}
			*budget--
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
				if seen[e.Inode] {
					continue
				}
				seen[e.Inode] = true
			}
			u.Bytes += align4K(e.Attr.Length)
			u.Inodes++
		}
	}
	return 0
}
