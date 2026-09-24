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

import "syscall"

// Shadowed-slice release, slice_data mode only.
//
// A slice stays in its chunk list, and so keeps its chunk_ref reference and
// its slice data counted, until compaction replaces it, even when later
// writes or a truncate cover every byte of it. A file rewritten with O_TRUNC
// would keep every earlier version counted until the chunk reaches the
// compaction trigger, which may never happen. After a Write that overlapped
// existing data of its chunk, and after a shrinking Truncate, the mount
// schedules a release of each affected chunk: in one metadata transaction the
// engine drops the slices with no visible byte from the chunk list and, with
// trash on (the Plori case), keeps their references in a delslices row, so
// they stay counted for trash-days like the slices compaction replaces; with
// trash off it removes their references. It reads and writes no object.
//
// A release and a compaction of the same chunk are deduplicated through
// compacting: a release requested while another job holds the chunk is
// recorded in ploriReleaseAgain and run by the holder when it finishes, so a
// request is never lost to a job that read the chunk list before the change.

// ploriShadowReleaser is implemented by the engines that keep slice_data
// accounting (the SQL engine).
type ploriShadowReleaser interface {
	// ploriChunkIndexes lists the chunk indexes of inode between first and
	// last, both included, that have a chunk row.
	ploriChunkIndexes(inode Ino, first, last uint32) ([]uint32, error)
	// doPloriReleaseShadowed drops the shadowed slices of one chunk.
	doPloriReleaseShadowed(inode Ino, indx uint32) syscall.Errno
}

// ploriReleaseAttempts is how many times a release rereads a chunk whose list
// changed between its read and its transaction. The job that changed the list
// schedules its own release when it shadowed anything.
const ploriReleaseAttempts = 3

// ploriShadowed reports for each slice of a chunk list whether none of its
// bytes is visible: later slices cover all of it. It runs buildSlice, the
// function reads use, over copies whose ids are replaced by their position
// in the list, so the visible pieces name the slices they come from. It
// returns nil when no slice is shadowed, and when every slice is (a list of
// zero-length entries), so the list is never emptied.
func ploriShadowed(ss []*slice) []bool {
	marked := make([]*slice, len(ss))
	for i, s := range ss {
		c := *s
		c.id = uint64(i) + 1
		c.left, c.right = nil, nil
		marked[i] = &c
	}
	visible := make([]bool, len(ss))
	for _, piece := range buildSlice(marked) {
		if piece.Id > 0 && piece.Len > 0 {
			visible[piece.Id-1] = true
		}
	}
	var shadowed, kept bool
	for _, v := range visible {
		if v {
			kept = true
		} else {
			shadowed = true
		}
	}
	if !shadowed || !kept {
		return nil
	}
	drop := make([]bool, len(ss))
	for i, v := range visible {
		drop[i] = !v
	}
	return drop
}

func ploriChunkKey(inode Ino, indx uint32) uint64 {
	return uint64(inode) + (uint64(indx) << 40)
}

// ploriScheduleRelease releases, in the background, the shadowed slices of
// the chunks of inode from first to last. It does nothing in logical mode.
func (m *baseMeta) ploriScheduleRelease(inode Ino, first, last uint32) {
	if !m.sliceData.Load() {
		return
	}
	r, ok := m.en.(ploriShadowReleaser)
	if !ok {
		return
	}
	m.ploriReleasing.Add(1)
	go func() {
		defer m.ploriReleasing.Add(-1)
		indexes := []uint32{first}
		if last > first {
			var err error
			if indexes, err = r.ploriChunkIndexes(inode, first, last); err != nil {
				logger.Warnf("release shadowed slices of inode %d chunks %d-%d: %s", inode, first, last, err)
				return
			}
		}
		for _, indx := range indexes {
			m.ploriReleaseChunk(r, inode, indx)
		}
	}()
}

// ploriReleaseChunk runs the release of one chunk unless another job holds
// the chunk, in which case the holder runs it when it finishes.
func (m *baseMeta) ploriReleaseChunk(r ploriShadowReleaser, inode Ino, indx uint32) {
	k := ploriChunkKey(inode, indx)
	m.Lock()
	if m.sessCtx != nil && m.sessCtx.Canceled() {
		m.Unlock()
		return
	}
	if m.compacting[k] {
		if m.ploriReleaseAgain == nil {
			m.ploriReleaseAgain = make(map[uint64]bool)
		}
		m.ploriReleaseAgain[k] = true
		m.Unlock()
		return
	}
	m.compacting[k] = true
	m.Unlock()
	for {
		if st := r.doPloriReleaseShadowed(inode, indx); st != 0 {
			logger.Warnf("release shadowed slices of inode %d chunk %d: %s", inode, indx, st)
		}
		m.Lock()
		if m.ploriReleaseAgain[k] {
			delete(m.ploriReleaseAgain, k)
			m.Unlock()
			continue
		}
		delete(m.compacting, k)
		m.Unlock()
		return
	}
}

// ploriReleaseRequested reports and clears a release recorded for chunk k
// while a compaction held it. The caller holds m's lock.
func (m *baseMeta) ploriReleaseRequested(k uint64) bool {
	if !m.ploriReleaseAgain[k] {
		return false
	}
	delete(m.ploriReleaseAgain, k)
	return true
}

// ploriCompactRefusedMax bounds the set of chunks whose refused compaction was
// logged. Past it a refusal is not logged, so no chunk is logged twice.
const ploriCompactRefusedMax = 4096

// ploriCompactRefusedLog logs once per chunk that compaction was refused by
// the data growth claim. A later successful claim forgets the chunk
// (ploriCompactClaimed).
func (m *baseMeta) ploriCompactRefusedLog(inode Ino, indx uint32, size uint32) {
	k := ploriChunkKey(inode, indx)
	m.Lock()
	if m.ploriCompactRefused[k] || len(m.ploriCompactRefused) >= ploriCompactRefusedMax {
		m.Unlock()
		return
	}
	if m.ploriCompactRefused == nil {
		m.ploriCompactRefused = make(map[uint64]bool)
	}
	m.ploriCompactRefused[k] = true
	m.Unlock()
	logger.Infof("compaction of inode %d chunk %d skipped: its new slice of %d bytes does not fit under the volume capacity", inode, indx, size)
}

// ploriCompactClaimed forgets a logged refusal of the chunk once its
// compaction claim succeeded.
func (m *baseMeta) ploriCompactClaimed(inode Ino, indx uint32) {
	m.Lock()
	if len(m.ploriCompactRefused) > 0 {
		delete(m.ploriCompactRefused, ploriChunkKey(inode, indx))
	}
	m.Unlock()
}
