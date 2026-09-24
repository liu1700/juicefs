//go:build !nosqlite || !nomysql || !nopg
// +build !nosqlite !nomysql !nopg

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
	"syscall"
	"time"

	"xorm.io/xorm"
)

// The SQL engine's part of the shadowed-slice release (plori_release.go).

func (m *dbMeta) ploriChunkIndexes(inode Ino, first, last uint32) ([]uint32, error) {
	var cs []chunk
	err := m.simpleTxn(Background(), func(s *xorm.Session) error {
		cs = cs[:0]
		return s.Cols("indx").Where("inode = ? AND indx >= ? AND indx <= ?", inode, first, last).Find(&cs)
	})
	if err != nil {
		return nil, err
	}
	indexes := make([]uint32, 0, len(cs))
	for _, c := range cs {
		indexes = append(indexes, c.Indx)
	}
	return indexes, nil
}

// doPloriReleaseShadowed reads the chunk list, finds the slices with no
// visible byte and drops them in ploriReleaseCommit. When the list changed
// between the read and the transaction, the commit refuses and the release
// reads again, up to ploriReleaseAttempts times.
func (m *dbMeta) doPloriReleaseShadowed(inode Ino, indx uint32) syscall.Errno {
	for attempt := 0; attempt < ploriReleaseAttempts; attempt++ {
		ss, st := m.doRead(Background(), inode, indx)
		if st != 0 {
			return st
		}
		if ss == nil {
			return syscall.EIO
		}
		drop := ploriShadowed(ss)
		if drop == nil {
			return 0
		}
		if st = m.ploriReleaseCommit(inode, indx, ss, drop); st != syscall.EINVAL {
			return st
		}
	}
	logger.Debugf("release shadowed slices of inode %d chunk %d: the chunk changed %d times, left to the next release", inode, indx, ploriReleaseAttempts)
	return 0
}

// ploriReleaseCommit removes the entries of ss marked in drop from the chunk
// list in one transaction. ss is the list read before the transaction; the
// transaction refuses with EINVAL unless the current list still starts with
// it, the check doCompactChunk makes. Entries appended after the read are
// kept: an appended slice only covers more bytes, so an entry hidden in ss
// stays hidden.
//
// With trash on, the dropped slices keep their references in a delslices row,
// counted and readable until doCleanupDelayedSlices removes them after
// trash-days, as compaction does. With trash off their references are
// removed, and a slice left without any is deleted.
func (m *dbMeta) ploriReleaseCommit(inode Ino, indx uint32, ss []*slice, drop []bool) syscall.Errno {
	origin := make([]byte, 0, len(ss)*sliceBytes)
	for _, s := range ss {
		origin = append(origin, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	trash := m.toTrash(0)
	var dropped []*slice
	var delayed []byte
	var entries int
	for i, s := range ss {
		if drop[i] {
			entries++
		}
		if !drop[i] || s.id == 0 {
			continue
		}
		dropped = append(dropped, s)
		if trash {
			delayed = append(delayed, m.encodeDelayedSlice(s.id, s.size)...)
		}
	}
	// A delslices row is keyed by a slice id; the id of a new slice that is
	// never written is unique.
	var key uint64
	if len(delayed) > 0 {
		if st := m.NewSlice(Background(), &key); st != 0 {
			return st
		}
	}
	var data int64
	st := errno(m.txn(func(s *xorm.Session) error {
		data = 0
		c := chunk{Inode: inode, Indx: indx}
		ok, err := s.ForUpdate().MustCols("indx").Get(&c)
		if err != nil {
			return err
		}
		if !ok || len(c.Slices) < len(origin) || !bytes.Equal(origin, c.Slices[:len(origin)]) {
			return syscall.EINVAL
		}
		kept := make([]byte, 0, len(c.Slices))
		for i := range ss {
			if !drop[i] {
				kept = append(kept, origin[i*sliceBytes:(i+1)*sliceBytes]...)
			}
		}
		kept = append(kept, c.Slices[len(origin):]...)
		c.Slices = kept
		if _, err := s.Cols("slices").Where("Inode = ? AND indx = ?", inode, indx).Update(c); err != nil {
			return err
		}
		if len(delayed) > 0 {
			if err := mustInsert(s, &delslices{key, time.Now().Unix(), delayed}); err != nil {
				return err
			}
		} else if !trash {
			for _, d := range dropped {
				n, err := m.ploriRefDelta(s, d.id, d.size, -1)
				if err != nil {
					return err
				}
				data += n
			}
		}
		m.genLog(Background(), s, time.Now().UnixNano(), "PLORI_RELEASE_SHADOWED(%d,%d,%d)", inode, indx, entries)
		return nil
	}, inode))
	if st != 0 {
		return st
	}
	m.applyDataSpace(data)
	m.of.InvalidateChunk(inode, indx)
	if !trash {
		for _, d := range dropped {
			var ref = sliceRef{Id: d.id}
			var ok bool
			err := m.simpleTxn(Background(), func(s *xorm.Session) error {
				var e error
				ok, e = s.Get(&ref)
				return e
			})
			if err == nil && ok && ref.Refs <= 0 {
				m.deleteSlice(d.id, d.size)
			}
		}
	}
	return 0
}
