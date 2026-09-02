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
	"math"
	"sync/atomic"
	"time"
)

// ploriFenced is the process-wide write fence. The Plori distribution runs one
// volume per process (`juicefs plori-mount`), so a process-wide flag and a
// per-volume flag are the same thing.
var ploriFenced atomic.Bool

// ploriStart anchors the monotonic clock for the lease deadline below. A
// time.Time captured with time.Now() carries a monotonic reading, and
// subtracting two of them measures elapsed process time regardless of what the
// wall clock did — which is the whole point of the deadline (threat-model.md
// §7.2: a writer that is SIGSTOPped or frozen must not resume believing it
// still holds its lease).
var ploriStart = time.Now()

// ploriWriteExpiry is the lease expiry expressed as nanoseconds of monotonic
// process time since ploriStart, so the check below is one atomic load plus one
// nanotime read — cheap enough to run on the write path itself rather than on a
// timer (PLO-323 F-5).
//
// noWriteExpiry rather than 0 marks "unset": a worker handed a lease that has
// already expired publishes a NEGATIVE offset, and that must revoke writes, not
// read as "no deadline".
const noWriteExpiry = math.MinInt64

var ploriWriteExpiry atomic.Int64

func init() {
	ploriWriteExpiry.Store(noWriteExpiry)
	writeBarrier = ploriWriteRevoked
}

// ploriWriteRevoked is the whole of Plori's write authority, in one predicate:
// this process may mutate the filesystem until it is fenced, or until the lease
// it was mounted under expires — whichever comes first. base.go's readOnly()
// consults it, so every gated operation re-checks both on every call.
func ploriWriteRevoked() bool {
	if ploriFenced.Load() {
		return true
	}
	if expiry := ploriWriteExpiry.Load(); expiry != noWriteExpiry {
		return int64(time.Since(ploriStart)) >= expiry
	}
	return false
}

// PloriFenceWrites revokes this process's right to mutate the filesystem. It
// is one-way: nothing un-fences a writer, because the authority that was lost
// is a lease epoch that is never reissued.
//
// After it returns, every mutating metadata operation answers EROFS —
// including the slice commit on a file handle opened before the fence
// (base.go Write/Truncate/SetAttr/Fallocate). That total shape is deliberate:
// it is the only thing that stops an Agent mid-`git clone` from committing
// through fds it already holds, and it is what preserves PLO-312's "invalidate
// handles on revocation" lesson (PLO-323 F-2 / acceptance A4).
//
// Because it is total, it is NOT the first step of an ordered stop. The
// ordered stop reserves the write-stop margin for a bounded flush INSIDE the
// lease (threat-model.md §7.5), and that flush commits slices through the same
// Write this seals. The supervisor therefore fences here only when the
// authority is genuinely gone — an out-of-band fence — and otherwise seals
// after the mount is detached (pkg/plori/mount/supervisor.go shutdown).
func PloriFenceWrites() { ploriFenced.Store(true) }

// PloriWritesFenced reports the fence state.
func PloriWritesFenced() bool { return ploriFenced.Load() }

// PloriSetWriteExpiry publishes the instant at which this process's lease dies,
// so every gated metadata operation can re-check it immediately before it runs
// rather than trusting a one-second timer (threat-model.md:812-815).
//
// `at` MUST come from arithmetic on a time.Now() value so it carries a
// monotonic reading; the mount supervisor derives it once per renewal from the
// control-plane's wall-clock expiry (pkg/plori/mount/lease.go).
//
// The instant is the lease EXPIRY, not `expiry − margin`. The margin is the
// tail of the lease reserved for the flush and the durability barrier, and the
// staged writeback drains through Write during it; sealing at the margin would
// make the bounded flush window §7.5 mandates impossible. The margin remains
// what it always was — the supervisor's trigger to stop the mount and start
// that drain.
func PloriSetWriteExpiry(at time.Time) {
	ploriWriteExpiry.Store(int64(at.Sub(ploriStart)))
}

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
