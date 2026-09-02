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

// ploriQuotaTrips counts how many times the VOLUME ceiling has refused an
// operation in this process. It is monotonic and never reset, so a reader can
// tell "the ceiling refused something since I last looked" from "the ceiling
// refused something once, an hour ago", which a boolean cannot.
var ploriQuotaTrips atomic.Uint64

func init() {
	volumeQuotaHook = func() { ploriQuotaTrips.Add(1) }
}

// PloriVolumeQuotaTrips is how many operations the volume ceiling has refused
// since this process started. The mount supervisor polls it on each renew tick
// and asks the control-plane to grow the grant when it has moved (PLO-324).
//
// It counts ONLY the volume ceiling — Format.Capacity and Format.Inodes, the
// two the control-plane's grant sets. A directory, user or group quota answers
// EDQUOT and is not a grant the allocator can raise, so counting it here would
// make the worker ask for capacity that would not help.
func PloriVolumeQuotaTrips() uint64 { return ploriQuotaTrips.Load() }

// PloriApplyGrant raises (or lowers) this mount's byte and inode ceiling
// without a remount, and without a second process touching the metadata.
//
// # Why this works, and why it is not `juicefs config`
//
// The ceiling checkQuota enforces is read from an atomic pointer on every
// single call — `m.getFormat()` (base.go), refused with ENOSPC in quota.go.
// Nothing caches it, so storing a new *Format makes the next operation obey the
// new number: there is no reload latency and no window in which half the mount
// enforces the old ceiling.
//
// `juicefs config --capacity` reaches the same field, but from a SEPARATE
// process with its own metadata client. On the Plori profile that is not
// available: the metadata engine is a local SQLite file replicated by
// Litestream, exactly one writer may have it open (ADR D3), and a second
// process opening it is the two-SQLite-instances hazard the ADR rules out. So
// the grant is applied by the process that already holds the session — this
// one — and the write is the same single `UPDATE setting` that command issues.
//
// The write to the engine is not optional bookkeeping. baseMeta.refresh
// (base.go) re-reads the stored Format on every heartbeat and calls setFormat
// with whatever it finds, so a purely in-memory ceiling would silently revert
// within one heartbeat (300 s on this profile). Persisting is what makes the
// change stick, and it costs one metadata transaction — see
// TestPloriApplyGrantCostsOneMetadataWrite.
//
// # Two refusals
//
// A non-positive ceiling is refused. JuiceFS reads `Capacity == 0` as
// UNLIMITED (cmd/format.go), so "no grant", "grant lost in transit" and
// "grant of zero" would all decode as "this Agent may consume the entire
// account budget". PLO-324's acceptance names this exactly: no failure mode may
// convert missing or zero configuration into unlimited storage.
//
// The stored Format is re-read from the engine rather than reusing the
// in-memory one, because the in-memory one has been through the storage
// credential patch: persisting it would write the bucket-wide object key into
// the database Litestream replicates (threat-model F-11). The credential fields
// are cleared again afterwards as a belt on that brace, since a Format loaded
// from an older, credentialed volume would otherwise be written straight back.
func PloriApplyGrant(m Meta, bytes, inodes int64) error {
	if bytes <= 0 || inodes <= 0 {
		return fmt.Errorf("plori: refusing a grant of %d bytes / %d inodes: zero means UNLIMITED in JuiceFS", bytes, inodes)
	}
	stored, err := m.Load(false)
	if err != nil {
		return fmt.Errorf("reload format: %w", err)
	}
	next := *stored
	next.Capacity = uint64(bytes)
	next.Inodes = uint64(inodes)
	next.AccessKey, next.SecretKey, next.SessionToken = "", "", ""
	next.KeyEncrypted = false
	if err := m.Init(&next, false); err != nil {
		return fmt.Errorf("apply grant: %w", err)
	}
	return nil
}
