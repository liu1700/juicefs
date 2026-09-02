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
	"sync/atomic"
)

// ploriFenced is the process-wide write fence. The Plori distribution runs one
// volume per process (`juicefs plori-mount`), so a process-wide flag and a
// per-volume flag are the same thing.
var ploriFenced atomic.Bool

func init() { writeBarrier = ploriFenced.Load }

// PloriFenceWrites revokes this process's right to mutate the filesystem. It
// is one-way: nothing un-fences a writer, because the authority that was lost
// is a lease epoch that is never reissued.
//
// After it returns, every mutating metadata operation answers EROFS. Reads and
// the flush of data already accepted continue, which is the point: the
// write-stop margin exists so the writeback cache can drain inside the lease
// it was written under (threat-model.md §7.2).
func PloriFenceWrites() { ploriFenced.Store(true) }

// PloriWritesFenced reports the fence state.
func PloriWritesFenced() bool { return ploriFenced.Load() }

// ploriSessionCleaner is the engine-level session cleanup the exported helper
// below drives. Both dbMeta and redisMeta implement it (pkg/meta/sql.go:3209,
// pkg/meta/redis.go); the assertion is legal here because this file is in
// package meta.
type ploriSessionCleaner interface {
	doCleanStaleSession(sid uint64) error
}

// PloriPurgeAllSessions deletes every recorded client session, releasing the
// POSIX locks and sustained inodes each one holds.
//
// CleanStaleSessions only collects sessions whose Expire has passed, and
// Expire is `now + 5*heartbeat` (base.go expireTime). At the Plori profile's
// --heartbeat 300 that is 25 minutes, so a restored metadata replica carries
// the previous writer's session — and its locks — for 25 minutes after the
// lease moved to a new writer. A restored replica has exactly one legitimate
// writer, so the correct sweep is total, not age-based (PLO-362).
//
// It must be called BEFORE the caller opens its own session, and it fails
// closed: a caller that cannot prove the sweep happened must refuse to mount.
func PloriPurgeAllSessions(m Meta) (int, error) {
	cleaner, ok := m.(ploriSessionCleaner)
	if !ok {
		return 0, fmt.Errorf("metadata engine %T cannot purge sessions", m)
	}
	sessions, err := m.ListSessions()
	if err != nil {
		return 0, fmt.Errorf("list sessions: %w", err)
	}
	var n int
	for _, s := range sessions {
		if err := cleaner.doCleanStaleSession(s.Sid); err != nil {
			return n, fmt.Errorf("clean session %d: %w", s.Sid, err)
		}
		n++
	}
	return n, nil
}
