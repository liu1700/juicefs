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
	"fmt"

	"xorm.io/xorm"
)

// ploriDataSpace is the counter row that holds the slice_data usage: the sum
// of chunk_ref.size over the rows whose refs is above zero.
//
// Every statement that inserts a chunk_ref row, changes refs or deletes a row
// goes through one of the helpers below. In slice_data mode each helper reads
// the affected rows before the change and adds size for a row whose refs moves
// from zero or below to above zero, and subtracts size for the opposite move,
// in the same transaction as the reference change. In logical mode the helpers
// run exactly the statements the engine ran before and touch no counter row.
//
// The row is rewritten from a full recount when a slice_data mount opens
// (ploriEnableSliceData), which covers a volume last mounted in logical mode,
// by an older fork, restored from an older replica point, or loaded from a
// dump. Paths that run in a separate process on an unmounted volume (juicefs
// load, the restore scanner) do not maintain the row for the same reason.
const ploriDataSpace = "ploriDataSpace"

const (
	stmtRefsIncr = "update chunk_ref set refs=refs+1 where chunkid = ? AND size = ?"
	stmtRefsDecr = "update chunk_ref set refs=refs-1 where chunkid=? AND size=?"
	stmtDataAdd  = "update counter set value=value + ? where name='ploriDataSpace'"
)

// refsCrossing is the change of ploriDataSpace when a row of size moves from
// refs old to refs new.
func refsCrossing(size uint32, old, new int) int64 {
	var d int64
	if new > 0 {
		d++
	}
	if old > 0 {
		d--
	}
	return d * int64(size)
}

// ploriAddDataSpace adds delta to the persisted counter row inside s.
func (m *dbMeta) ploriAddDataSpace(s *xorm.Session, delta int64) error {
	if delta == 0 {
		return nil
	}
	_, err := s.Exec(m.sqlConv(stmtDataAdd), delta)
	return err
}

// ploriRefDelta changes the refs of the row (id, size) by delta, which is 1 or
// -1, with the statement the engine has always used for that change: a row
// whose size does not match is left alone. It returns the change of
// ploriDataSpace, which the caller counts in memory with applyDataSpace once
// the transaction has committed.
func (m *dbMeta) ploriRefDelta(s *xorm.Session, id uint64, size uint32, delta int) (int64, error) {
	stmt := stmtRefsIncr
	if delta < 0 {
		stmt = stmtRefsDecr
	}
	if !m.sliceData.Load() {
		_, err := s.Exec(m.sqlConv(stmt), id, size)
		return 0, err
	}
	var ref sliceRef
	found, err := s.Where("chunkid = ? AND size = ?", id, size).ForUpdate().Get(&ref)
	if err != nil {
		return 0, err
	}
	if _, err = s.Exec(m.sqlConv(stmt), id, size); err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	d := refsCrossing(size, ref.Refs, ref.Refs+delta)
	return d, m.ploriAddDataSpace(s, d)
}

// ploriInsertRef inserts a chunk_ref row. A row inserted with refs above zero
// adds its size; the row compaction inserts with refs 0 for a slice it did not
// use adds nothing.
func (m *dbMeta) ploriInsertRef(s *xorm.Session, ref sliceRef) (int64, error) {
	if err := mustInsert(s, ref); err != nil {
		return 0, err
	}
	if !m.sliceData.Load() || ref.Refs <= 0 {
		return 0, nil
	}
	d := int64(ref.Size)
	return d, m.ploriAddDataSpace(s, d)
}

// ploriDeleteRef deletes the chunk_ref row of id once its objects are gone.
// Every caller deletes a row it read at refs zero or below, so the change is
// normally zero; a row still above zero is subtracted so the counter keeps
// matching the recount.
func (m *dbMeta) ploriDeleteRef(s *xorm.Session, id uint64) (int64, error) {
	if !m.sliceData.Load() {
		_, err := s.Delete(&sliceRef{Id: id})
		return 0, err
	}
	var ref sliceRef
	found, err := s.Where("chunkid = ?", id).ForUpdate().Get(&ref)
	if err != nil {
		return 0, err
	}
	if _, err = s.Delete(&sliceRef{Id: id}); err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	d := refsCrossing(ref.Size, ref.Refs, 0)
	return d, m.ploriAddDataSpace(s, d)
}

// ploriBatchRefsCrossing reads the rows of one batchUpdateChunkRefs batch
// before its update and returns the ploriDataSpace change the update makes.
func (m *dbMeta) ploriBatchRefsCrossing(s *xorm.Session, batch []uint64, deltas map[uint64]int) (int64, error) {
	var rows []sliceRef
	if err := s.In("chunkid", batch).ForUpdate().Find(&rows); err != nil {
		return 0, err
	}
	var d int64
	for _, r := range rows {
		d += refsCrossing(r.Size, r.Refs, r.Refs+deltas[r.Id])
	}
	return d, nil
}

// ploriRecountDataSpace computes SUM(size) over the rows with refs above zero
// and, unless the client is read-only, writes it into the counter row. It
// returns the recount, the value the row held and whether the row existed.
func (m *dbMeta) ploriRecountDataSpace() (sum, persisted int64, existed bool, err error) {
	read := func(s *xorm.Session) error {
		total, err := s.Where("refs > 0").SumInt(&sliceRef{}, "size")
		if err != nil {
			return fmt.Errorf("sum slice data: %w", err)
		}
		c := counter{Name: ploriDataSpace}
		ok, err := s.Get(&c)
		if err != nil {
			return fmt.Errorf("get counter %s: %w", ploriDataSpace, err)
		}
		sum, persisted, existed = total, c.Value, ok
		return nil
	}
	if m.conf.ReadOnly {
		err = m.simpleTxn(Background(), read)
		return
	}
	err = m.txn(func(s *xorm.Session) error {
		if err := read(s); err != nil {
			return err
		}
		c := counter{Name: ploriDataSpace, Value: sum}
		if existed {
			_, err := s.Cols("value").Update(&c, &counter{Name: ploriDataSpace})
			return err
		}
		return mustInsert(s, &c)
	})
	return
}

// ploriEnableSliceData switches this client to slice_data accounting. The
// mode is set before the recount, so a reference change that commits after
// the recount's transaction also changes the row, and the recount then sets
// the in-memory value. The caller must run it before the session starts any
// background job and before any call is served: a change that committed
// before the recount and is counted in memory after it would be counted twice.
func (m *dbMeta) ploriEnableSliceData() (recount, drift int64, existed bool, err error) {
	m.sliceData.Store(true)
	sum, persisted, existed, err := m.ploriRecountDataSpace()
	if err != nil {
		m.sliceData.Store(false)
		return 0, 0, false, err
	}
	m.volMu.Lock()
	m.dataSpace.Store(sum)
	m.volMu.Unlock()
	if existed {
		drift = sum - persisted
	}
	return sum, drift, existed, nil
}
