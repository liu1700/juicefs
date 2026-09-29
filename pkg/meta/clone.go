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
	"sync"
	"syscall"
)

type cloneLinksKey struct{}
type cloneLinkKey struct{}
type cloneDetachedRootKey struct{}

// The map is immutable after preflight. Each entry serializes creation and
// publication of one destination inode across concurrent directory workers.
// Only committed inodes are published; failed transactions leave ino zero.
type cloneLink struct {
	sync.Mutex
	ino Ino
}

type cloneLinks map[Ino]*cloneLink

func cloneLinksFrom(ctx Context) cloneLinks {
	links, _ := ctx.Value(cloneLinksKey{}).(cloneLinks)
	return links
}

// cloneSummary counts paths for directory quotas and distinct inodes for
// volume/user/group quotas and reservations. Like the clone itself, this walk
// is not a snapshot: callers must quiesce the source for a consistent copy.
func (m *baseMeta) cloneSummary(ctx Context, inode Ino, attr *Attr, paths, unique *Summary, links cloneLinks) syscall.Errno {
	if st := m.cloneAllowed(ctx); st != 0 {
		return st
	}
	add := func(s *Summary) {
		s.Size += uint64(align4K(attr.Length))
		if attr.Typ == TypeDirectory {
			s.Dirs++
		} else {
			s.Files++
		}
		if attr.Typ == TypeFile {
			s.Length += attr.Length
		}
	}
	add(paths)
	if attr.Typ != TypeDirectory && attr.Nlink > 1 {
		// Reject before workers can obscure ENOTSUP with sibling cancellation.
		// The backend also checks transactionally if the source changes later.
		if !m.cloneHardlinks {
			return syscall.ENOTSUP
		}
		if links[inode] != nil {
			return 0
		}
		links[inode] = &cloneLink{}
	}
	add(unique)
	if attr.Typ != TypeDirectory {
		return 0
	}
	var entries []*Entry
	if st := m.en.doReaddir(ctx, inode, 1, &entries, -1); st != 0 {
		return st
	}
	for _, e := range entries {
		if st := m.cloneSummary(ctx, e.Inode, e.Attr, paths, unique, links); st != 0 {
			return st
		}
	}
	return 0
}
