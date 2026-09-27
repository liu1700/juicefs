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

import "xorm.io/xorm"

// This file carries the SQL build tags of sql.go rather than no tag, because it
// names dbMeta. PloriMeasureTrash (untagged) reaches it through the
// ploriTrashSliceReader interface, so a build without any SQL engine still
// compiles the walk and only loses the slice_data branch, which no other engine
// can be in.

// ploriTrashQueryBatch bounds the IN lists below. SQLite before 3.32 accepts at
// most 999 bound parameters per statement.
const ploriTrashQueryBatch = 500

// ploriTrashReclaimable implements ploriTrashSliceReader.
//
// A slice's `refs` counts one reference per occurrence in a chunk list (write,
// clone and copy_file_range each add one per occurrence; deleteChunk removes one
// per occurrence) plus one per delslices hold. Counting the occurrences in the
// chunk lists of `files` gives the references a purge of those files removes; a
// slice is released by the purge exactly when that count equals `refs`. A slice
// also held by a live file, a clone, or a delslices entry has more references and
// is not counted. Rows at refs <= 0 are already outside the slice_data figure and
// cannot match, because every counted slice has at least one trash reference.
//
// The chunk lists and the chunk_ref rows are read in one read-only transaction,
// so the two agree with each other. A file purged between the directory walk and
// this transaction has no chunk rows left and adds nothing.
func (m *dbMeta) ploriTrashReclaimable(ctx Context, files []Ino) (int64, error) {
	var reclaimable int64
	err := m.roTxn(ctx, func(s *xorm.Session) error {
		reclaimable = 0
		trashRefs := make(map[uint64]int)
		for start := 0; start < len(files); start += ploriTrashQueryBatch {
			var chunks []chunk
			if err := s.Cols("slices").In("inode", files[start:min(start+ploriTrashQueryBatch, len(files))]).Find(&chunks); err != nil {
				return err
			}
			for _, c := range chunks {
				for _, sl := range readSliceBuf(c.Slices) {
					if sl.id > 0 {
						trashRefs[sl.id]++
					}
				}
			}
		}
		ids := make([]uint64, 0, len(trashRefs))
		for id := range trashRefs {
			ids = append(ids, id)
		}
		for start := 0; start < len(ids); start += ploriTrashQueryBatch {
			var refs []sliceRef
			if err := s.In("chunkid", ids[start:min(start+ploriTrashQueryBatch, len(ids))]).Find(&refs); err != nil {
				return err
			}
			for _, r := range refs {
				if trashRefs[r.Id] == r.Refs {
					reclaimable += int64(r.Size)
				}
			}
		}
		return nil
	})
	return reclaimable, err
}
