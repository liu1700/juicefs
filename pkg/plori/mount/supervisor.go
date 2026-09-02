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

package mount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Deps are everything the supervisor talks to. Each one is an interface so the
// whole state machine runs in a unit test without FUSE, an object store or a
// control-plane.
type Deps struct {
	FS         FS
	CP         ControlPlane
	Replicator Replicator
	Fencer     Fencer
	// Credentials owns the object key: it re-reads the file the key arrives
	// in, hands the new pair to every S3 client in the process at once, and
	// bounds how long the worker keeps serving a key the store refuses
	// (PLO-322). It may be nil in tests that never touch the object store, in
	// which case the credential tick does nothing.
	Credentials *CredentialWatcher
	// ControlGateInstalled reports whether the `.control` uid gate is
	// compiled in. It is a function rather than a bool so the check is made
	// against the live vfs package, not against a value someone set.
	ControlGateInstalled func() bool
	Now                  func() time.Time
	Log                  func(event string, kv ...any)
}

// Supervisor owns one volume for the lifetime of the process.
type Supervisor struct {
	Spec  *MountSpec
	Paths Paths
	Deps  Deps

	// Options is the resolved mount_options vocabulary. cmd fills it so the
	// operator override is applied exactly once, at startup.
	Options MountOptions

	deadline *Deadline
	vol      Volume
	// drain is the measured answer to "how long is the backlog in front of a
	// stop". It is what turns the write-stop margin from a constant into a
	// bound (PLO-383).
	drain *DrainModel

	mu              sync.Mutex
	lastBarrier     BarrierResult
	lastTxID        string
	grantApplied    int64
	pendingAck      int64
	quotaTrips      uint64
	quotaExhausted  bool
	growAsked       bool
	restoredUnclean bool
	lastUsage       Usage
	lastRenewOK     bool
	fenced          bool
	formattedHere   bool
	// leaseTTL is the full lease length as the control-plane last issued it,
	// observed rather than configured: the worker is never told the TTL, but
	// every renewal's answer is one measurement of it. It bounds how early the
	// ordered stop may begin.
	leaseTTL time.Duration
	// backlogCap is the staging cap currently pushed down to the chunk store.
	backlogCap int64
	// replFailedSince is when the replication probe first failed without a
	// success since. Zero means replication is believed healthy. It is the
	// only state PLO-411 needs: one instant answers both "is health.json's
	// replication_failed true" and "has this outlasted a barrier period".
	replFailedSince time.Time
	// replRestarted records that the one repair attempt for the current
	// failure has been made, so a replicator that cannot be revived is not
	// restarted every health tick until the stop trips.
	replRestarted bool
}

func (s *Supervisor) now() time.Time {
	if s.Deps.Now != nil {
		return s.Deps.Now()
	}
	return time.Now()
}

func (s *Supervisor) log(event string, kv ...any) {
	if s.Deps.Log != nil {
		s.Deps.Log(event, kv...)
	}
}

// Run is the whole lifecycle: start, serve, stop. It returns a *Fatal whose
// Exit is what the process exits with.
func (s *Supervisor) Run(ctx context.Context, stop <-chan os.Signal) *Fatal {
	if err := s.start(ctx); err != nil {
		f := Classify(err)
		// A refusal after the lease was already taken must hand the lease
		// back, or the Agent waits a full TTL for a mount that never was.
		s.releaseLease(reasonFor(f))
		return f
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.vol.Serve(ctx) }()

	if err := s.vol.AwaitMounted(ctx); err != nil {
		f := fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, true, "mount did not become ready: %s", err)
		s.shutdown(context.Background(), "mount_failed")
		return f
	}
	if err := writeJSONAtomic(s.Paths.ReadyPath(), Ready{
		Epoch:     s.Spec.FenceEpoch,
		MountedAt: s.now().UTC(),
		Volume:    s.Spec.StorageVolumeID,
	}); err != nil {
		f := fatalf(CodeRefused, ErrCodeRestoreFailed, false, "write ready file: %s", err)
		s.shutdown(context.Background(), "ready_file_failed")
		return f
	}
	s.log("ready", "volume", s.Spec.StorageVolumeID, "epoch", s.Spec.FenceEpoch)

	return s.loop(ctx, stop, serveErr)
}

// ---------------------------------------------------------------- startup ---

func (s *Supervisor) start(ctx context.Context) error {
	s.deadline = NewDeadline(s.Spec.LeaseExpiresAt, s.Spec.WriteStopMargin.D(), s.now())
	s.drain = NewDrainModel(DefaultDrainPerBlock)
	// The spec does not carry the lease TTL, so seed it from what is left of
	// the lease the spec arrived with. That UNDERSTATES the real TTL by
	// however long the spec took to reach this process, and understating it
	// only makes the stop start later and the backlog cap tighter -- the safe
	// direction. The first renewal replaces it with the exact figure.
	s.setLeaseTTL(s.Spec.LeaseExpiresAt.Sub(s.now().UTC()))

	if err := os.MkdirAll(s.Paths.StateDir, 0o700); err != nil {
		return fatalf(CodeRefused, ErrCodeRestoreFailed, false, "create state dir: %s", err)
	}
	// 0700 is re-applied rather than assumed: the directory may survive a Pod
	// UID recycle, and a permissive mode on it exposes the SQLite metadata and
	// the writeback staging area to anything else running on the node.
	if err := os.Chmod(s.Paths.StateDir, 0o700); err != nil {
		return fatalf(CodeRefused, ErrCodeRestoreFailed, false, "chmod state dir: %s", err)
	}
	if err := os.MkdirAll(s.Paths.CacheDir, 0o700); err != nil {
		return fatalf(CodeRefused, ErrCodeRestoreFailed, false, "create cache dir: %s", err)
	}

	// The fence marker is the store-side proof of epoch ownership and must be
	// claimed BEFORE the first LTX upload, so it goes first — before restore,
	// before anything is written under meta_prefix.
	marker, err := json.Marshal(FenceMarker{
		Volume:    s.Spec.StorageVolumeID,
		Epoch:     s.Spec.FenceEpoch,
		ClaimedAt: s.now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fatalf(CodeRefused, ErrCodeRestoreFailed, false, "encode fence marker: %s", err)
	}
	if err := s.Deps.Fencer.Claim(ctx, s.Spec.FenceMarkerKey, marker); err != nil {
		if errors.Is(err, ErrFenceMarkerHeld) {
			if rerr := s.reclaimOwnMarker(ctx); rerr != nil {
				return rerr
			}
		} else {
			return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true, "claim fence marker: %s", err)
		}
	} else {
		s.log("fence_marker_claimed", "key", s.Spec.FenceMarkerKey)
	}

	if err := s.restoreOrFormat(ctx); err != nil {
		return err
	}

	vol, err := s.Deps.FS.Open(ctx, s.Spec)
	if err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "open restored metadata: %s", err)
	}
	s.vol = vol

	if err := vol.IntegrityCheck(ctx); err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreIntegrity, false, "metadata integrity: %s", err)
	}
	if err := s.identityMatches(ctx); err != nil {
		return err
	}
	if err := s.refusals(ctx); err != nil {
		return err
	}
	// PLO-362: a restored database still lists the previous writer's session.
	// With --heartbeat 300 its row does not expire for 25 minutes
	// (pkg/meta/base.go:876-881 expireTime = heartbeat*5), and until then it
	// holds POSIX locks and sustained inodes on behalf of a writer the lease
	// has already replaced. A restored replica has exactly one legitimate
	// writer — this process — so every recorded session is swept before the
	// mount opens its own.
	n, err := vol.PurgeSessions(ctx)
	if err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "purge stale sessions: %s", err)
	}
	s.log("sessions_purged", "count", n)

	if err := s.repairAfterUncleanStop(ctx); err != nil {
		return err
	}

	if err := s.Deps.Replicator.Start(ctx); err != nil {
		return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true, "start replication: %s", err)
	}
	if s.formattedHere {
		// A freshly formatted volume has no replica yet. Push one before the
		// Agent can write, so a crash in the first second still restores to a
		// real filesystem instead of looking unformatted and being formatted
		// a second time.
		if err := s.Deps.Replicator.SyncAndWait(ctx); err != nil {
			return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true, "seed replica: %s", err)
		}
		// The seed sync just completed, so reading the position now IS reading
		// it at T_before: nothing has been written since (PLO-416).
		s.reportDurablePoint(ctx, BarrierResult{DurableAt: s.now().UTC(), BarrierAt: s.now().UTC()}, s.anchorTxID(ctx))
	}
	// The spec's grant is authoritative; the restored replica's Format carries
	// whatever ceiling the PREVIOUS generation persisted, which the allocator
	// may have moved while this volume had no writer. Applying it before the
	// mount serves anything is one metadata write (~1.1 ms, zero object
	// requests — meta.TestPloriApplyGrantCostsOneMetadataWrite) and it removes
	// the window in which a resumed Agent enforces a stale ceiling.
	if g := s.Spec.Grant; g.Epoch > 0 {
		s.applyGrant(ctx, g)
	}
	return nil
}

// reclaimOwnMarker decides whether the 412 on the fence marker is this
// worker's own claim at the same epoch, in which case the epoch is already
// ours and the mount may proceed. Anything it cannot prove stays fenced out.
//
// The case is ordinary rather than exotic, which is why it is worth the two
// round trips: the worker crashes, nothing releases the lease — nothing else
// may (PLO-323 W4) — the kubelet retries NodePublish, and the control-plane
// replays the SAME epoch for the same Pod (storagespec/issuer.go). Before this,
// the new worker's `If-None-Match: *` hit the marker its own predecessor wrote
// and the volume stayed unmountable for the rest of the lease TTL, on exactly
// the failure this supervisor exists to survive (F-6).
//
// Two proofs, both fail-closed:
//
//  1. the marker names this volume and this epoch. The key already encodes
//     both, so this only rejects a marker whose body disagrees with where it
//     lives — a store this worker will not reason about further;
//  2. the control-plane confirms that THIS process is the live holder of that
//     epoch. That is the holder identity the audit asked for, taken from the
//     layer that owns it rather than from a string in the marker body: the
//     renew route authenticates the worker's projected ServiceAccount token and
//     refuses unless the open lease's holder Pod UID is the caller's own and
//     its epoch is the one presented (issuer.go authorizeHolder). A replayed
//     spec, a stranger and a moved-past epoch each get a typed refusal.
//
// What this cannot check is that no OTHER process is alive at the same epoch —
// a wedged predecessor still mounted and still writing would pass both proofs.
// The plugin owns that boundary: a mounted-but-stale worker must be signalled
// and detached BEFORE a new mount-spec is requested (PLO-392). The same-epoch
// reclaim is safe because of that guarantee, not in spite of it.
func (s *Supervisor) reclaimOwnMarker(ctx context.Context) *Fatal {
	held := fatalf(CodeFenced, ErrCodeFenceMarkerHeld, false,
		"another writer already claimed epoch %d (%s)", s.Spec.FenceEpoch, s.Spec.FenceMarkerKey)

	m, err := s.Deps.Fencer.ReadMarker(ctx, s.Spec.FenceMarkerKey)
	if err != nil {
		s.log("fence_marker_unreadable", "key", s.Spec.FenceMarkerKey, "error", err.Error())
		return held
	}
	if m.Volume != s.Spec.StorageVolumeID || m.Epoch != s.Spec.FenceEpoch {
		s.log("fence_marker_foreign", "key", s.Spec.FenceMarkerKey,
			"marker_volume", m.Volume, "marker_epoch", m.Epoch)
		return held
	}
	// An empty request: this renew is an identity PROOF, not a grant
	// conversation. Nothing has been applied yet — the volume is not even open
	// — so there is no acknowledgement to carry and nothing to ask for.
	if _, err := s.Deps.CP.RenewLease(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, RenewRequest{}); err != nil {
		s.log("fence_marker_not_ours", "key", s.Spec.FenceMarkerKey, "error", err.Error())
		return held
	}
	s.log("fence_marker_reclaimed", "key", s.Spec.FenceMarkerKey,
		"epoch", s.Spec.FenceEpoch, "claimed_at", m.ClaimedAt)
	return nil
}

func (s *Supervisor) restoreOrFormat(ctx context.Context) error {
	// There are up to two known durable points, and the newer one wins WHOLE:
	// the prefix to restore from, the replica transaction to stop at and the
	// instant that transaction was durable at all come from the same point,
	// never one from each.
	//
	//   * the control-plane's, in the MountSpec. Authoritative, and the only
	//     one a Pod rescheduled onto a different node can see.
	//   * the local `durable-point.json`, left by whichever generation last ran
	//     HERE. reportDurablePoint persists it BEFORE it posts, deliberately,
	//     so a generation that barriers and then loses the network leaves a
	//     local point the server never heard about.
	//
	// That last case is why they must not be mixed. Taking the local anchor
	// with the server's prefix would restore epoch N-2's replica up to a point
	// epoch N-1 established, silently dropping everything N-1 wrote — worse
	// than either source alone. Epoch is the comparison, not the timestamp: it
	// is the one coordinate in this design that does not depend on a clock.
	//
	// With neither, the restore takes the replica's latest transaction, which
	// is what every mount did before the server carried a point (PLO-391).
	var (
		anchor time.Time
		txid   string
		source string
		from   int64
	)
	if dp := s.Spec.DurablePoint; dp != nil {
		anchor, txid, from, source = dp.DurableAt, dp.ReplicaTxID, dp.FenceEpoch, s.Spec.RestoreFromPrefix
		s.log("restore_anchor", "durable_at", anchor, "replica_txid", txid, "from_epoch", from, "known_by", "mount_spec")
	}
	// `<=`, not `<`: a worker restarted at the SAME epoch — the crash-restart
	// the kubelet produces, where the issuer replays the epoch for the same Pod
	// — left this file itself, and it is the newest anchor there is. Restoring
	// such a restart to the previous epoch's point drops everything epoch N had
	// already made durable (PLO-323 F-6c).
	localPoint, _ := ReadDurablePoint(s.Paths.DurablePointPath())
	if dp := localPoint; dp != nil {
		if dp.Volume == s.Spec.StorageVolumeID && dp.FenceEpoch <= s.Spec.FenceEpoch && dp.FenceEpoch > from {
			// Its prefix is populated by construction: the file is written
			// only after a barrier, and a barrier only happens once
			// replication is running under that epoch's prefix.
			anchor, txid, from, source = dp.DurableAt, dp.ReplicaTxID, dp.FenceEpoch, s.Spec.MetaPrefixForEpoch(dp.FenceEpoch)
			s.log("restore_anchor", "durable_at", anchor, "replica_txid", txid, "from_epoch", from, "known_by", "state_dir")
		}
	}
	// A previous generation that did not write the clean marker died without
	// finishing its stop, so the restored image is only durable up to its last
	// barrier. crash-consistency.md §7 Rank 1 says the repair for that is a
	// full fsck of the restored metadata against the objects; PLO-320 owes it
	// (PLO-316 wave 2 measured 870 ms / 12 LIST / 34 MiB on 11k objects, so it
	// is affordable unconditionally — never path-scoped, which is 15x worse).
	// Until then the condition is reported, not repaired.
	cleanStop := fileExists(s.Paths.CleanStopPath())
	s.restoredUnclean = !cleanStop
	_ = os.Remove(s.Paths.CleanStopPath())

	// The state directory is a host path and outlives the Pod, so on any mount
	// after the first on this node there is already a `meta.db` here. Decide
	// between it and the replica ONCE, on evidence, rather than refusing at the
	// restore (PLO-422): a database this worker's own predecessor left behind
	// after a clean stop is the newest copy there is, and restoring over a
	// live or newer one is what must never happen.
	var serverEpoch int64
	if dp := s.Spec.DurablePoint; dp != nil {
		serverEpoch = dp.FenceEpoch
	}
	verdict, why, rerr := reconcileLocalDatabase(s.Paths, s.Spec.StorageVolumeID, cleanStop, localPoint, serverEpoch)
	if rerr != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "reconcile the local metadata database: %s", rerr)
	}
	if verdict != localDBAbsent {
		s.log("local_database", "verdict", string(verdict), "reason", why)
	}
	if verdict == localDBAdopted {
		// No restore: the local database is at least as durable as the replica,
		// and the identity check that runs next proves it is this volume's.
		return nil
	}

	// The metadata root is partitioned per writer epoch, so a FRESH epoch's own
	// prefix is empty by construction and the bytes live under an earlier one.
	// The prefix of the winning durable point above IS that earlier one, which
	// is why the source is settled there rather than derived a second time.
	//
	// With no durable point at all the source is discovered by listing the
	// metadata root and taking the newest prefix AT OR BELOW this epoch that
	// holds more than a fence marker. That listing stays: it is the only correct
	// answer for a volume no writer has ever reported a durable point for, and
	// it is what makes `epoch - 1` unnecessary — an epoch that claimed a fence
	// and replicated nothing is skipped by both paths. "At or below" is what
	// makes the crash-restart case correct: a worker that replicated under
	// epoch N and died before posting its first durable point comes back at N,
	// and `g<N>/` is by then the newest history there is (PLO-323 F-6c).
	if source != "" {
		s.log("restore_source", "prefix", source, "discovery", "durable_point")
	} else {
		var err error
		if source, err = s.Deps.Fencer.PriorMetaPrefix(ctx, s.Spec.MetaRoot(), s.Spec.FenceEpoch); err != nil {
			return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true,
				"find the metadata generation to restore from: %s", err)
		}
		if source != "" {
			s.log("restore_source", "prefix", source, "discovery", "list")
		}
	}

	// The transaction id wins over the wall clock when the point carries one:
	// it names a transaction, where a timestamp names a FILE whose stamp is
	// the moment it was encoded, strictly after the last commit inside it
	// (PLO-396, RestoreOptions).
	err := s.Deps.Replicator.Restore(ctx, source, RestoreOptions{TXID: txid, Timestamp: anchor})
	switch {
	case err == nil:
		if s.restoredUnclean {
			s.log("unclean_generation", "error", ErrCodeRestoredToBarrier,
				"restored_to", anchor, "repair", "pending")
		}
		return nil
	case errors.Is(err, ErrReplicaEmpty):
		return s.formatFirstBoot(ctx)
	default:
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "restore metadata replica: %s", err)
	}
}

// formatFirstBoot is the only path that creates a filesystem, and it is
// deliberately narrow. An empty replica on a volume that is not on its first
// migration generation, or that the control-plane already believes active,
// means the replica was lost, not that the volume is new — and formatting
// there would silently replace a filesystem with an empty one.
func (s *Supervisor) formatFirstBoot(ctx context.Context) error {
	if s.Spec.Generation != 1 {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false,
			"metadata replica is empty at migration generation %d; refusing to format over a lost replica",
			s.Spec.Generation)
	}
	switch s.Spec.VolumeState {
	case VolumeStateFormatted, VolumeStateAllocating:
	default:
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false,
			"metadata replica is empty but volume state is %q; refusing to format", s.Spec.VolumeState)
	}
	// may_format is the control-plane's authorisation, and it is the only thing
	// consulted here: the server sets it exactly when the volume has never been
	// formatted, and Validate has already refused a spec where it disagrees with
	// the recorded Format.UUID.
	if !s.Spec.MayFormat {
		return fatalf(CodeIdentityMismatch, ErrCodeIdentityMismatch, false,
			"metadata replica is empty but the control-plane did not authorise a format (format UUID %q)",
			s.Spec.FormatUUID)
	}
	if err := s.Deps.FS.Format(ctx, s.Spec); err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "format volume: %s", err)
	}
	s.formattedHere = true
	s.log("formatted", "volume", s.Spec.StorageVolumeID, "trash_days", s.Spec.Format.TrashDays)
	return nil
}

// identityMatches is ADR D2's three-way match. All three legs are required:
// the spec says which volume this mount is for, the Format says which
// filesystem the restored metadata is, and the `juicefs_uuid` object says
// which filesystem owns the data prefix. Two of three agreeing is exactly the
// state a swapped replica produces.
func (s *Supervisor) identityMatches(ctx context.Context) error {
	id := s.vol.Identity()
	want := s.Spec.VolumeName()
	if id.Name != want {
		return fatalf(CodeIdentityMismatch, ErrCodeIdentityMismatch, false,
			"restored Format.Name %q does not match the spec's data prefix %q", id.Name, want)
	}
	if s.Spec.FormatUUID != "" && id.UUID != s.Spec.FormatUUID {
		return fatalf(CodeIdentityMismatch, ErrCodeIdentityMismatch, false,
			"restored Format.UUID %s does not match the spec's %s", id.UUID, s.Spec.FormatUUID)
	}
	stored, err := s.vol.StoredUUID(ctx)
	if err != nil {
		return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true,
			"read juicefs_uuid under the data prefix: %s", err)
	}
	if stored != id.UUID {
		return fatalf(CodeIdentityMismatch, ErrCodeIdentityMismatch, false,
			"data prefix belongs to volume %s but the restored metadata claims %s", stored, id.UUID)
	}
	return nil
}

// refusals are the fail-closed startup checks the M0 hand-off assigned to this
// process. Every one of them exits 70: the mount is refusable, not retryable,
// and a retry would produce the same answer.
func (s *Supervisor) refusals(ctx context.Context) error {
	if s.vol.Identity().TrashDays == 0 {
		return fatalf(CodeRefused, ErrCodeVolumeTrashDisabled, false,
			"volume has trash-days 0; crash-consistency Rank 1 needs at least %d", DefaultTrashDays)
	}
	if s.Deps.ControlGateInstalled == nil || !s.Deps.ControlGateInstalled() {
		return fatalf(CodeRefused, ErrCodeControlWritable, false,
			"the .control uid gate is not installed; every internal command would be Agent-writable")
	}
	if err := checkCacheDirTenant(s.Paths.CacheDir, s.vol.Identity().UUID); err != nil {
		return fatalf(CodeRefused, ErrCodeCacheDirTenantMismatch, false, "%s", err)
	}
	return nil
}

// repairAfterUncleanStop is crash-consistency.md §7 d3.
//
// A generation that did not write the clean marker died before its ordered
// stop finished, so the metadata Litestream replicated can reference blocks
// the writeback cache never uploaded. Those files stat fine and read EIO —
// the exact crux the M0 harness reproduced — and no existing JuiceFS command
// repairs them: `fsck` detects lost objects but its --repair only fixes
// directories (cmd/fsck.go:59-76).
//
// It runs unconditionally after an unclean generation and never after a clean
// one: PLO-316 wave 2 measured a full scan at 870 ms / 12 LIST / 34 MiB on
// 11k objects, so the full scan is affordable and a path-scoped one is 15x
// worse. It runs before replication starts so the repair is inside the first
// transaction this epoch replicates.
//
// A repair failure is fatal and not retryable in place: serving a filesystem
// whose damage was neither bounded nor recorded is worse than refusing to
// mount it.
func (s *Supervisor) repairAfterUncleanStop(ctx context.Context) error {
	if !s.restoredUnclean || s.formattedHere {
		return nil
	}
	report, err := s.vol.RepairAfterRestore(ctx)
	if err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoredToBarrier, false,
			"repair the restored volume: %s", err)
	}
	s.log("restore_repair", "error", ErrCodeRestoredToBarrier,
		"scanned", report.Scanned, "checked", report.Checked,
		"missing", report.Missing, "files", report.Files,
		"truncated", report.Truncated, "elapsed_ms", report.Elapsed.Milliseconds())
	return nil
}

// checkCacheDirTenant refuses a cache directory that already holds another
// volume's staged blocks.
//
// JuiceFS puts the per-volume cache at `<cache-dir>/<Format.UUID>/`
// (pkg/chunk/cached_store.go:588-593 SelfCheck) with `rawstaging/` inside it.
// A cache dir reused across tenants means this mount would find, and upload,
// blocks staged by a different filesystem under this mount's credentials
// (disk_cache.go:1015 scanStaging). One cache dir per volume UUID is the
// contract; this is the check that proves it held.
func checkCacheDirTenant(cacheDir, uuid string) error {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read cache dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == uuid {
			continue
		}
		staging := filepath.Join(cacheDir, e.Name(), "rawstaging")
		empty, err := dirIsEmpty(staging)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", staging, err)
		}
		if !empty {
			return fmt.Errorf("cache dir %s holds staged blocks for volume %s, not %s", cacheDir, e.Name(), uuid)
		}
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dirIsEmpty(dir string) (bool, error) {
	found := false
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return !found, nil
}

// -------------------------------------------------------------- run loop ---

func (s *Supervisor) loop(ctx context.Context, stop <-chan os.Signal, serveErr <-chan error) *Fatal {
	// Arm the metadata engine's own deadline before the first Agent write can
	// reach it. From here the engine re-checks it on every gated operation, so
	// a process that is frozen past its expiry and thawed cannot commit — the
	// guard ticker below is only what decides when to STOP the mount, not what
	// decides whether a write may be submitted (PLO-323 F-5).
	s.publishWriteExpiry()
	// Push the first backlog cap down before the Agent can write a byte. Until
	// this call the chunk store is unlimited, which is upstream's behaviour and
	// the state PLO-346 measured 1,008 staged blocks in.
	s.retuneBacklog()

	renew := time.NewTicker(s.Spec.LeaseRenewInterval.D())
	defer renew.Stop()
	barrier := time.NewTicker(s.barrierInterval())
	defer barrier.Stop()
	// The deadline guard runs far more often than the renew interval: it is
	// the backstop for the case where renewals stop happening at all (the
	// control-plane is unreachable), where no response ever arrives to move
	// the deadline forward.
	guard := time.NewTicker(time.Second)
	defer guard.Stop()
	// health.json is rewritten on its own cadence rather than only on a renew
	// tick: the plugin reads a file older than 60 s as degraded, and a long
	// renew interval must not make a healthy mount look stale.
	health := time.NewTicker(HealthWriteInterval)
	defer health.Stop()
	// The credential is re-read on its own ticker rather than on the renew
	// tick: the two answer to different clocks. Renewal is the control-plane's
	// lease cadence; this one is how fast a key rolled on the node reaches a
	// running mount, and coupling them would make a slower renew interval
	// silently slow rotation down as well.
	credential := time.NewTicker(s.Deps.Credentials.Interval())
	defer credential.Stop()

	ticks := 0
	renewedAt := s.now()
	for {
		select {
		case <-stop:
			s.log("sigterm")
			return s.shutdown(context.Background(), ReasonShutdown)

		case err := <-serveErr:
			// The FUSE session ended without us asking. JuiceFS's own child
			// supervisor is not in the picture — the loop runs in this
			// process — so nothing else will notice, and the mount point is
			// potentially half-attached. It is never a clean exit: a
			// zero-status session end is still a mount that stopped serving,
			// and an unrecognised mount failure is fenced, not retryable
			// (parity-matrix §4a #3).
			s.shutdown(context.Background(), "fuse_session_ended")
			if err == nil {
				err = errors.New("session closed without an error")
			}
			return fatalf(CodeFenced, ErrCodeLeaseLost, false, "fuse session ended: %s", err)

		case <-guard.C:
			now := s.now()
			if jump := ClockJump(renewedAt, now); jump > MaxClockJump {
				s.log("clock_jump", "delta", jump.String())
				return s.fenceAndStop(fatalf(CodeFenced, ErrCodeLeaseLost, false,
					"wall clock moved %s relative to the monotonic clock; treating as a fence trip", jump), ReasonFenced)
			}
			// The ordered stop begins at the write-stop margin MINUS however
			// long the backlog in front of it is projected to take, which is
			// threat-model.md §7.5's third instant. With an empty queue the
			// projection is zero and this is exactly the old margin trigger;
			// with a queue it is the only way the drain the margin was
			// supposed to pay for actually fits inside the lease (PLO-383).
			if early := s.stopEarliness(); s.deadline.StopDue(now, early) {
				s.log("write_stop_margin_reached",
					"pending_blocks", s.vol.PendingBlocks(),
					"projected_drain", s.drain.Project(s.vol.PendingBlocks()).String(),
					"started_early_by", early.String())
				return s.fenceAndStop(fatalf(CodeFenced, ErrCodeLeaseLost, false,
					"lease deadline reached without a successful renewal"), ReasonFenced)
			}

		case <-renew.C:
			ticks++
			before := s.now()
			s.noteQuotaTrips()
			resp, err := s.Deps.CP.RenewLease(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, s.renewRequest())
			if err != nil {
				var cpErr *CPError
				if errors.As(err, &cpErr) && cpErr.Fenced() {
					// Terminal by contract: stale_epoch on renew is never
					// retried, because a retry is the fenced writer still
					// believing it holds the volume. It is also the
					// OUT-OF-BAND case — the epoch was taken away rather than
					// run out — so the stop skips the barrier and the final
					// sync entirely (F-1).
					s.setRenewOK(false)
					return s.fenceAndStop(fatalf(CodeFenced, ErrCodeFencedOutOfBand, false,
						"lease lost: %s", cpErr), ReasonFencedOutOfBand)
				}
				s.setRenewOK(false)
				s.log("renew_failed", "error", err.Error())
				s.writeHealth()
				continue
			}
			s.setRenewOK(true)
			s.ackDelivered(resp.Grant.AckedEpoch)
			renewedAt = before
			s.setLeaseTTL(resp.LeaseExpiresAt.Sub(before.UTC()))
			s.deadline.Update(resp.LeaseExpiresAt, s.Spec.WriteStopMargin.D(), before)
			s.publishWriteExpiry()
			s.retuneBacklog()
			if resp.Grant.Epoch > s.appliedGrant() {
				s.applyGrant(ctx, resp.Grant)
			} else if resp.OverBudget {
				s.growRefused()
			}
			if ticks%DefaultUsageReportEvery == 0 {
				s.reportUsage(ctx)
			}
			s.writeHealth()

		case <-health.C:
			// The replication check runs on the health tick and not on its
			// own, because health.json is where its verdict is published and
			// because this select is the supervisor's only goroutine: a
			// check here cannot overlap a barrier, a credential reload or a
			// stop, which is what makes the repair safe to attempt from it
			// (PLO-411).
			if f := s.checkReplication(ctx); f != nil {
				return f
			}
			s.writeHealth()

		case <-credential.C:
			if f := s.pollCredential(ctx); f != nil {
				return f
			}

		case <-barrier.C:
			s.runBarrier(ctx)
		}
	}
}

// pollCredential re-reads the object key and decides whether this worker can
// still reach the store. It returns non-nil only when the worker must stop.
//
// It runs in the supervisor's own goroutine, which is what makes the
// replicator restart safe: a barrier, a shutdown and this cannot overlap.
func (s *Supervisor) pollCredential(ctx context.Context) *Fatal {
	w := s.Deps.Credentials
	if w == nil {
		return nil
	}
	if w.Poll() {
		s.reloadReplicatorCredentials(ctx)
	}
	if w.Verdict() != CredentialRejected {
		return nil
	}
	// The store has refused this key for the whole grace and no new one has
	// arrived. Stop the way an out-of-band fence stops — nothing that needs
	// the store — but report it as the retryable object-store class, because
	// the lease was never lost: the plugin should republish and the next
	// worker will pick up whatever key the node holds by then.
	s.log("credential_rejected_stop", "grace", CredentialRejectGrace.String())
	if stopErr := s.shutdown(context.Background(), ReasonCredentialRejected); stopErr.Exit == CodeBarrierIncomplete {
		return stopErr
	}
	return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true,
		"object store refused this worker's credential for %s", CredentialRejectGrace)
}

// reloadReplicatorCredentials hands the new key to the replicator, if it is
// the kind that needs handing one.
func (s *Supervisor) reloadReplicatorCredentials(ctx context.Context) {
	r, ok := s.Deps.Replicator.(ReplicatorReloader)
	if !ok {
		return
	}
	if err := r.ReloadCredentials(ctx); err != nil {
		// Not fatal on its own: replication falling behind is visible in
		// replica_lag_ms and recovers on the next successful sync, whereas
		// stopping the mount over it would cost the Agent its session for a
		// condition the next tick may fix.
		s.log("credential_replicator_reload_failed", "error", err.Error())
	}
}

// checkReplication asks the replicator whether it is still replicating this
// worker's database, repairs it once, and stops the mount if it stays dead.
//
// The rule is one instant, not a state machine: replFailedSince is set by the
// first failing probe and cleared by the first succeeding one. From it come
// both the health field the plugin republishes and the stop, which trips when
// replication has been off for longer than a barrier period — the same window
// the barrier itself would have exposed the failure in, had anything on that
// path checked. Before this existed nothing did: the replicator's exit was
// read only by Stop and Abort, so a Litestream that died on its own left a
// mount serving writes with no metadata replica and a green health file.
//
// Stopping is the right answer rather than an over-reaction. ADR B1 makes
// Litestream the metadata backup: a mount replicating nothing is accumulating
// exactly the loss the whole design exists to bound, and it is doing it
// invisibly. The stop is ORDERED — barrier, unmount, lease released — so the
// Agent loses its session and nothing else, and the exit is the same
// data-loss-reported class a missed barrier gets (69), because that is what
// it is.
func (s *Supervisor) checkReplication(ctx context.Context) *Fatal {
	sup, ok := s.Deps.Replicator.(ReplicationSupervisor)
	if !ok {
		return nil
	}
	err := sup.Probe(ctx)
	if err == nil {
		s.mu.Lock()
		wasFailing := !s.replFailedSince.IsZero()
		s.replFailedSince, s.replRestarted = time.Time{}, false
		s.mu.Unlock()
		if wasFailing {
			s.log("replication_recovered")
		}
		return nil
	}

	now := s.now()
	s.mu.Lock()
	if s.replFailedSince.IsZero() {
		s.replFailedSince = now
	}
	since, restarted := s.replFailedSince, s.replRestarted
	s.mu.Unlock()

	failedFor := now.Sub(since)
	if failedFor >= s.barrierInterval() {
		s.log("replication_failed_stop", "error", err.Error(), "failed_for", failedFor.String())
		// Write the verdict out before the stop begins: the ordered stop can
		// take the whole write-stop margin, and an operator reading
		// health.json during it should see why.
		s.writeHealth()
		return s.shutdown(ctx, ReasonReplicationFailed)
	}

	s.log("replication_probe_failed", "error", err.Error(), "failed_for", failedFor.String())
	if restarted {
		return nil
	}
	s.mu.Lock()
	s.replRestarted = true
	s.mu.Unlock()
	if rerr := sup.Restart(ctx); rerr != nil {
		s.log("replication_restart_failed", "error", rerr.Error())
		return nil
	}
	s.log("replication_restarted")
	return nil
}

func (s *Supervisor) barrierInterval() time.Duration {
	if s.Options.BarrierInterval > 0 {
		return s.Options.BarrierInterval
	}
	return DefaultBarrierInterval
}

func (s *Supervisor) runBarrier(ctx context.Context) {
	// A barrier must not outlive the authority that permits it
	// (crash-consistency Q7): bound it by whatever is left of the lease.
	budget := s.deadline.RemainingLease(s.now())
	if budget <= 0 {
		return
	}
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// T_before is captured BEFORE the barrier, and it is the value persisted
	// and reported. crash-consistency.md §5: the barrier's own completion
	// time is not a safe restore point.
	tBefore := s.now().UTC()
	// The anchor's txid is read HERE, at T_before, and not after the barrier
	// (PLO-416). See anchorTxID.
	txid := s.anchorTxID(bctx)
	// The periodic barrier IS a drain of the live backlog, so it is the one
	// honest measurement of how long a drain takes on this node, under this
	// workload, right now. Sampling anything else would be a model; this is an
	// observation (PLO-383).
	pendingBefore, startedAt := s.vol.PendingBlocks(), s.now()
	res, err := s.vol.Barrier(bctx)
	if err != nil {
		s.log("barrier_failed", "error", err.Error())
		return
	}
	s.observeDrain(pendingBefore, s.now().Sub(startedAt))
	res.DurableAt = tBefore
	if res.BarrierAt.IsZero() {
		res.BarrierAt = s.now().UTC()
	}
	s.reportDurablePoint(ctx, res, txid)
}

// anchorTxID is the replica position that goes with T_before.
//
// It is read BEFORE the barrier runs, and that ordering is the whole point
// (PLO-416). The durable point promises that a restore to it lands on a tree
// whose every block exists in the object store. A txid read AFTER the barrier
// breaks that promise: the mount is live throughout the barrier, so a
// transaction committed while the barrier was running is in the post-barrier
// replica position, and the blocks it references were staged after the
// barrier began force-queueing — the barrier never waited on them (ADR B4).
// Until fork #47 that anchor was unusable anyway (the txid failed to decode
// and every restore fell back to the timestamp); now that restores prefer it,
// a post-barrier txid is an anchor pointing at blocks that may not exist.
//
// Reading it here is safe in the one direction that matters. `sync -wait`
// completes before the barrier starts, so everything it captured was
// committed before the barrier began, and the barrier force-queues everything
// staged at its start — which is exactly that set. The anchor can therefore
// only be at or behind the true durable frontier, never ahead of it.
//
// A failure is not fatal: DurableAt alone is still a usable restore point
// (the timestamp path), so the anchor degrades to what it was before #47
// rather than aborting a barrier that is about to make real data durable.
func (s *Supervisor) anchorTxID(ctx context.Context) string {
	txid, err := s.Deps.Replicator.TxID(ctx)
	if err != nil {
		s.log("replica_txid_unavailable", "error", err.Error())
		return ""
	}
	return txid
}

func (s *Supervisor) reportDurablePoint(ctx context.Context, res BarrierResult, txid string) {
	dp := DurablePoint{
		Volume:      s.Spec.StorageVolumeID,
		FenceEpoch:  s.Spec.FenceEpoch,
		DurableAt:   res.DurableAt,
		BarrierAt:   res.BarrierAt,
		ReplicaTxID: txid,
	}
	// Persist locally first. The local copy is what the next generation on
	// THIS node restores to; the control-plane copy is what a different node
	// would need, and losing the network must not lose the anchor.
	if err := writeJSONAtomic(s.Paths.DurablePointPath(), dp); err != nil {
		s.log("durable_point_persist_failed", "error", err.Error())
	}
	s.mu.Lock()
	s.lastBarrier, s.lastTxID = res, txid
	s.mu.Unlock()
	if err := s.Deps.CP.ReportDurablePoint(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, res, txid); err != nil {
		s.log("durable_point_report_failed", "error", err.Error())
	}
}

// applyGrant enforces a new ceiling locally and queues the acknowledgement.
//
// The apply is synchronous and the ack is not: the ack rides the next renew
// (renewRequest below), because renewal is the only regular round trip this
// process makes and a second HTTP call to say "done" is a second
// authorisation of a fact the lease already proves. The lag it costs is one
// renew interval, and it is a SAFE lag in one direction only — the allocator
// counts an un-acknowledged grant as the larger of issued and acknowledged, so
// an increase in flight is reserved but not double-spent, and a DECREASE is
// never issued to a volume a writer holds at all (storagequota
// ErrGrantHeldByWriter).
//
// A failed apply leaves grantApplied where it was, so the next renew carries
// the same grant and tries again. That is the correct retry: the ceiling the
// control-plane issued is not enforced until this succeeds, and pretending
// otherwise would let the allocator hand the difference to a sibling.
func (s *Supervisor) applyGrant(ctx context.Context, g GrantSpec) {
	if err := s.vol.ApplyGrant(ctx, g.Bytes, g.Inodes); err != nil {
		s.log("grant_apply_failed", "epoch", g.Epoch, "error", err.Error())
		return
	}
	s.mu.Lock()
	s.grantApplied = g.Epoch
	if g.Epoch > g.AckedEpoch {
		s.pendingAck = g.Epoch
	}
	// A larger ceiling is the answer to whatever refused the last write, so the
	// exhausted state and the outstanding request both close here. If the new
	// ceiling is still too small the very next refusal reopens them.
	s.quotaExhausted = false
	s.growAsked = false
	s.mu.Unlock()
	s.log("grant_applied", "epoch", g.Epoch, "bytes", g.Bytes, "inodes", g.Inodes)
}

// noteQuotaTrips samples the metadata engine's refusal counter. A counter
// rather than a flag because the engine has no idea what a grant epoch is: the
// supervisor has to be able to tell "the ceiling refused something SINCE I last
// looked" from "the ceiling refused something at some point", and only a
// monotonic number carries that.
func (s *Supervisor) noteQuotaTrips() {
	n := s.vol.QuotaTrips()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > s.quotaTrips {
		s.quotaTrips = n
		s.quotaExhausted = true
	}
}

// renewRequest is what this tick tells the control-plane about the grant: the
// epoch it has applied since the last renew, and whether it needs more room.
//
// The Grow flag is raised at most once per grant epoch. The ceiling refuses
// every write of a filesystem that is full — a `git clone` against a full
// volume trips it thousands of times a second — so a request per refusal would
// be a request storm against a per-owner advisory lock. One per epoch is
// enough because the answer to the request is a new epoch.
func (s *Supervisor) renewRequest() RenewRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	req := RenewRequest{AckedGrantEpoch: s.pendingAck}
	if s.quotaExhausted && !s.growAsked {
		req.Grow = true
		s.growAsked = true
	}
	return req
}

// growRefused records that the account could not fund the last request, which
// re-arms it.
//
// Re-arming looks like the storm the once-per-epoch rule exists to prevent, and
// is not: the request rides a renew that was going to happen anyway, so it
// costs no round trip, and it is bounded at one per renew interval per volume
// that is genuinely full. It is also the only way out of the state. Buying disk
// raises the account budget, and the billing hook's Rebalance reclaims and
// compacts — it does not GROW a volume that is already at its ceiling
// (storagequota.Rebalance). So an Agent that asked once, was refused, and never
// asked again would stay stuck after the user paid to unstick it.
func (s *Supervisor) growRefused() {
	s.mu.Lock()
	s.growAsked = false
	epoch := s.grantApplied
	s.mu.Unlock()
	s.log("grant_over_budget", "epoch", epoch)
}

func (s *Supervisor) reportUsage(ctx context.Context) {
	u, err := s.vol.Usage(ctx)
	if err != nil {
		s.log("usage_read_failed", "error", err.Error())
		return
	}
	s.mu.Lock()
	s.lastUsage = u
	s.mu.Unlock()
	if err := s.Deps.CP.ReportUsage(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, u, s.now().UTC()); err != nil {
		s.log("usage_report_failed", "error", err.Error())
	}
}

// --------------------------------------------------------------- shutdown ---

// Terminal reasons handed to /lease/release, and the flag that decides the
// stop's shape. They are constants rather than literals because shutdown
// branches on ReasonFencedOutOfBand: it is the difference between a bounded
// flush and no uploads at all.
const (
	// ReasonShutdown — SIGTERM with the lease healthy.
	ReasonShutdown = "shutdown"
	// ReasonFenced — the worker's own deadline ran out: the write-stop margin
	// was reached without a successful renewal, or the clock jumped. The
	// authority was not taken away, it expired, so the ordered stop runs with
	// its bounded flush (threat-model.md §7.5).
	ReasonFenced = "fenced"
	// ReasonFencedOutOfBand — somebody else holds this volume: a 412 on the
	// fence marker whose body is not ours, or stale_epoch/lease_held from a
	// renew. The epoch was taken away, so the stop uploads nothing (F-1).
	ReasonFencedOutOfBand = "fenced_out_of_band"
	// ReasonReplicationFailed — the metadata replica stopped receiving this
	// database and did not come back. The stop is fully ORDERED (barrier,
	// unmount, final sync, lease released), because everything that makes a
	// stop safe still works: it is only the continuous replication that is
	// gone, and the barrier + final sync are the one chance left to make the
	// current metadata durable (PLO-411).
	ReasonReplicationFailed = "replication_failed"
)

// fenceAndStop is the lease-loss path. `reason` decides the shape of the stop:
// an out-of-band fence seals the filesystem immediately and leaves without
// touching the store; a deadline trip runs the ordered stop.
func (s *Supervisor) fenceAndStop(f *Fatal, reason string) *Fatal {
	if reason == ReasonFencedOutOfBand {
		// Seal now: this writer provably no longer owns the epoch, so nothing
		// it still holds open may commit — not one more slice (F-2 + F-1).
		s.vol.FenceWrites()
	}
	s.mu.Lock()
	s.fenced = true
	s.mu.Unlock()
	if stopErr := s.shutdown(context.Background(), reason); stopErr.Exit == CodeBarrierIncomplete {
		// Losing data is the more serious of the two facts, so it wins the
		// exit code; the fence is still reported in the message.
		return &Fatal{
			Exit:    CodeBarrierIncomplete,
			ErrCode: ErrCodeBarrierIncomplete,
			Err:     fmt.Errorf("%s (after %s)", stopErr.Err, f.Err),
		}
	}
	return f
}

// shutdown is the ordered stop of ADR / PLO-326: fence new operations, run the
// durability barrier, unmount and close SQLite, final replication sync, report
// the durable point and usage, release the lease.
//
// The whole sequence is bounded by what is left of the lease, because a
// barrier that outlives its authority is exactly the fault PLO-323 fault 4
// names. When the bound is exhausted the worker exits 69 — reported data
// loss — and still releases the lease, because holding it costs the Agent a
// full TTL and buys nothing: the data is already lost either way. (PLO-326 B2
// asks for "fail visibly WITHOUT releasing"; with F-2's seal a failed stop
// cannot still be writing, so the amended bullet is "fail visibly, fenced,
// then release" — threat-model.md §7.)
//
// `reason` chooses between two shapes:
//
//   - ORDERED (shutdown, fenced, and the startup failures): steps 1-7 as
//     written. The write-stop margin exists to pay for exactly this drain, so
//     the metadata seal is NOT step 1 — sealing before the barrier would make
//     FlushAll answer EROFS and turn every clean stop into reported data loss.
//     The seal lands the moment the mount is detached, which is the first
//     instant at which no further filesystem request can arrive.
//   - OUT OF BAND (ReasonFencedOutOfBand): the epoch belongs to somebody else,
//     so steps 3 and 5 are skipped entirely and the mount is detached without
//     a flush. A barrier here would push staged blocks into the SHARED data
//     prefix this writer no longer owns, and a final sync would push LTX into
//     the metadata prefix its successor restores from — history that
//     references blocks the skipped barrier never uploaded (F-1 ruling,
//     threat-model.md §7).
func (s *Supervisor) shutdown(ctx context.Context, reason string) *Fatal {
	// A rejected credential takes the same shape for a different reason: steps
	// 3 and 5 are object-store writes and the store is what is refusing this
	// process, so running them would spend the whole remaining lease failing
	// and report data loss for a condition that is retryable (PLO-322).
	outOfBand := reason == ReasonFencedOutOfBand || reason == ReasonCredentialRejected

	budget := s.deadline.RemainingLease(s.now())
	if budget < time.Second {
		budget = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	var incomplete error

	// 1. fence new operations. Out of band the seal already happened in
	// fenceAndStop, before anything else could run; repeating it here is
	// idempotent and keeps this function correct on its own terms.
	if outOfBand {
		s.vol.FenceWrites()
	}
	s.mu.Lock()
	s.fenced = true
	s.mu.Unlock()

	// 2 + 3. drain and run the remote durability barrier
	// res stays zero out of band, and step 6 skips the report: no barrier ran,
	// so there is no new durable point to name.
	var res BarrierResult
	var pendingBefore uint64
	var anchorTxID string
	if !outOfBand {
		tBefore := s.now().UTC()
		// Same ordering as the periodic barrier, and here it also fixes a
		// second-order bug: step 5 stops the replicator, so a txid read in
		// step 6 was always read from a replicator that had already exited
		// (PLO-416).
		anchorTxID = s.anchorTxID(ctx)
		pendingBefore = s.vol.PendingBlocks()
		startedAt := s.now()
		var err error
		res, err = s.vol.Barrier(ctx)
		if err != nil {
			incomplete = fmt.Errorf("durability barrier: %w", err)
			s.log("shutdown_barrier_failed", "error", err.Error(),
				"pending_blocks_at_start", pendingBefore, "budget", budget.String())
		} else {
			// A completed stop barrier is the most informative drain sample
			// there is -- it is the exact operation the projection exists to
			// predict -- so it feeds the model even though this process is
			// about to exit: the model is what sizes the cap, and the cap is
			// read back by whatever mounts this volume next.
			s.observeDrain(pendingBefore, s.now().Sub(startedAt))
		}
		res.DurableAt = tBefore
		if res.BarrierAt.IsZero() {
			res.BarrierAt = s.now().UTC()
		}
	}

	// 4. unmount, then close SQLite. Barrier failure already blocks unmount
	// upstream (cmd/umount.go:120-125); that fail-closed behaviour is kept.
	// Out of band the mount is DETACHED instead — `umount --flush` would push
	// the staged writeback into a data prefix this writer no longer owns.
	if outOfBand {
		if err := s.vol.Detach(ctx); err != nil {
			s.log("shutdown_detach_failed", "error", err.Error())
		}
	} else {
		if err := s.vol.Unmount(ctx); err != nil && incomplete == nil {
			incomplete = fmt.Errorf("unmount: %w", err)
		}
		// Seal the metadata engine now that no further filesystem request can
		// arrive. Everything after this point — Close, the final sync — runs
		// against a filesystem nothing can mutate.
		s.vol.FenceWrites()
	}
	if err := s.vol.Close(); err != nil && incomplete == nil {
		incomplete = fmt.Errorf("close metadata: %w", err)
	}

	// 5. final replication sync, then stop the replicator (which performs its
	// own shutdown sync). Out of band both are skipped and the replicator is
	// killed instead, so nothing this writer staged reaches its epoch's prefix.
	if outOfBand {
		if err := s.Deps.Replicator.Abort(ctx); err != nil {
			s.log("shutdown_replicator_abort_failed", "error", err.Error())
		}
	} else {
		if err := s.Deps.Replicator.SyncAndWait(ctx); err != nil && incomplete == nil {
			incomplete = fmt.Errorf("final replica sync: %w", err)
		}
		if err := s.Deps.Replicator.Stop(ctx); err != nil && incomplete == nil {
			incomplete = fmt.Errorf("stop replication: %w", err)
		}
	}

	// 6. report the durable point and the final usage. Both are best effort:
	// they inform the control-plane, they do not make anything durable. Out of
	// band there is nothing truthful to report: no barrier ran, so no new
	// durable point exists, and a usage figure for a volume somebody else owns
	// would overwrite the successor's.
	if incomplete == nil && !outOfBand {
		s.reportDurablePoint(context.WithoutCancel(ctx), res, anchorTxID)
		s.reportUsage(context.WithoutCancel(ctx))
	}

	// 7. release the writer lease, always.
	s.releaseLease(reason)

	if reason == ReasonCredentialRejected {
		// Also not a clean stop: no `clean` marker, so the next generation
		// repairs. The caller supplies the message; this exists so the shape
		// and the exit code cannot drift apart.
		return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true,
			"object credential rejected; stopped without a barrier or a final sync")
	}
	if outOfBand {
		// Not a clean stop and not a data-loss report: the epoch was taken
		// away. No `clean` marker, so the next generation repairs.
		return fatalf(CodeFenced, ErrCodeFencedOutOfBand, false,
			"fenced out of band; stopped without a barrier or a final sync")
	}
	if incomplete != nil {
		// The shortfall is measured, not asserted: how much time the stop had,
		// how deep the backlog was, and how much of it is still staged. Exit 69
		// already means "data was lost"; PLO-383 makes it say how much, because
		// "the margin was not enough" is only actionable with the number that
		// would have been (benchmark-real-node.md §5).
		return fatalf(CodeBarrierIncomplete, ErrCodeBarrierIncomplete, false,
			"stop did not complete inside the write-stop window (%s): %s",
			s.shortfall(budget, pendingBefore), incomplete)
	}
	if reason == ReasonReplicationFailed {
		// A clean, ordered stop that is still a data-loss report: the barrier
		// and the final sync above are the last thing this generation could
		// do, and whatever was written between the last successful sync and
		// them reached the replica only if the final sync worked. Same exit
		// class as a missed barrier for the same reason — the plugin's map
		// already routes 69 to "unpublish and surface a typed event", which
		// is the handling this needs (PLO-411).
		return fatalf(CodeBarrierIncomplete, ErrCodeReplicationFailed, false,
			"metadata replication stopped and did not recover within a barrier period; stopped after a final barrier and sync")
	}
	// The marker is written last and only here, so its absence at the next
	// start is exactly "the previous generation did not finish its stop".
	if err := os.WriteFile(s.Paths.CleanStopPath(), []byte(s.now().UTC().Format(time.RFC3339Nano)+"\n"), 0o600); err != nil {
		s.log("clean_marker_write_failed", "error", err.Error())
	}
	return &Fatal{Exit: CodeOK, Err: errors.New("clean stop")}
}

// publishWriteExpiry hands the metadata engine the instant its write gate must
// refuse at. It is called once before the loop starts and again after every
// renewal, so the engine's own view of the deadline can never be older than the
// last acknowledged renewal.
func (s *Supervisor) publishWriteExpiry() {
	if s.vol == nil {
		return
	}
	s.vol.SetWriteExpiry(s.deadline.Expiry())
}

// ---------------------------------------------------------- drain model ---

// setLeaseTTL records the full lease length as the control-plane last issued
// it. Nothing tells the worker the TTL; every renewal answer is a measurement
// of it, and this is where that measurement is kept.
func (s *Supervisor) setLeaseTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	s.leaseTTL = ttl
	s.mu.Unlock()
}

func (s *Supervisor) currentLeaseTTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaseTTL
}

// maxStopEarliness bounds how much earlier than the write-stop margin the
// ordered stop is allowed to begin.
//
// Without a bound the projection alone decides, and a projection large enough
// to reach past a whole lease would make the stop due the instant the lease is
// renewed -- tearing down a mount that is renewing normally, which is a worse
// failure than the one being fixed. The bound is stated as renewals rather
// than as a duration: leave two renew intervals between the last successful
// renewal and the earliest possible stop, so the stop still takes two
// consecutive renewal failures to trigger, exactly as it did when the trigger
// was the bare margin.
func (s *Supervisor) maxStopEarliness() time.Duration {
	early := s.currentLeaseTTL() - s.Spec.WriteStopMargin.D() - 2*s.Spec.LeaseRenewInterval.D()
	if early < 0 {
		return 0
	}
	return early
}

// stopEarliness is how much earlier than the margin the stop begins right now:
// the projected drain of the live backlog, clamped to what the lease can pay
// for. Zero -- an empty queue, or a lease with no room -- is the old behaviour
// exactly.
func (s *Supervisor) stopEarliness() time.Duration {
	projected := s.drain.Project(s.vol.PendingBlocks())
	if maxEarly := s.maxStopEarliness(); projected > maxEarly {
		return maxEarly
	}
	return projected
}

// drainBudget is the time the ordered stop is guaranteed for its drain: the
// margin, plus however much earlier than the margin it is allowed to begin.
// It is what the backlog cap is sized against, so that the deepest backlog the
// store will hold is always one this budget can drain.
func (s *Supervisor) drainBudget() time.Duration {
	return s.Spec.WriteStopMargin.D() + s.maxStopEarliness()
}

// observeDrain folds a completed barrier into the model and re-sizes the cap
// from it. The two belong together: a new measurement that did not move the
// cap would leave the store holding a backlog the new measurement says is too
// deep.
func (s *Supervisor) observeDrain(blocks uint64, elapsed time.Duration) {
	before := s.drain.PerBlock()
	s.drain.Observe(blocks, elapsed)
	if after := s.drain.PerBlock(); after != before {
		s.log("drain_rate_measured", "blocks", blocks, "elapsed", elapsed.String(),
			"per_block", after.String(), "blocks_per_second", s.drain.RatePerSecond())
	}
	s.retuneBacklog()
}

// retuneBacklog pushes the current cap down to the chunk store.
//
// This is the back-pressure half of PLO-383, and it is deliberately the same
// mechanism as the projection rather than a second one: the cap is whatever
// backlog the measured drain rate says still fits in the stop's budget, never
// more than the profile ceiling. A store at its cap uploads through instead of
// staging, so the writer waits for the object store and the backlog stops
// growing -- no error, no dropped data, and no EROFS on a mount whose lease is
// perfectly healthy.
func (s *Supervisor) retuneBacklog() {
	if s.vol == nil {
		return
	}
	want := s.drain.CapForBudget(s.drainBudget(), DefaultMaxStagingBacklog)
	s.mu.Lock()
	changed := want != s.backlogCap
	s.backlogCap = want
	s.mu.Unlock()
	if changed {
		s.log("staging_backlog_cap", "blocks", want, "budget", s.drainBudget().String(),
			"per_block", s.drain.PerBlock().String())
	}
	s.vol.SetStagingBacklogCap(want)
}

// shortfall describes, in one string, why a stop ran out of time: what it had,
// what it was asked to drain, and what is still staged.
func (s *Supervisor) shortfall(budget time.Duration, pendingAtStart uint64) string {
	remaining := s.vol.PendingBlocks()
	return fmt.Sprintf("budget %s, %d blocks staged at the start and %d still staged, projected %s at %.1f blocks/s",
		budget.Round(time.Millisecond), pendingAtStart, remaining,
		s.drain.Project(remaining).Round(time.Millisecond), s.drain.RatePerSecond())
}

func (s *Supervisor) releaseLease(reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Deps.CP.ReleaseLease(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, reason); err != nil {
		s.log("lease_release_failed", "reason", reason, "error", err.Error())
		return
	}
	s.log("lease_released", "reason", reason)
}

func reasonFor(f *Fatal) string {
	switch f.Exit {
	case CodeIdentityMismatch:
		return "identity_mismatch"
	case CodeFenced:
		// A marker held by somebody else is the startup half of the same fact
		// the renew route reports as stale_epoch: this volume is not ours.
		if f.ErrCode == ErrCodeFenceMarkerHeld || f.ErrCode == ErrCodeFencedOutOfBand {
			return ReasonFencedOutOfBand
		}
		return ReasonFenced
	case CodeRestoreFailed:
		return "restore_failed"
	case CodeObjectStore:
		return "object_store_unreachable"
	case CodeRefused:
		return strings.ToLower(strings.TrimPrefix(f.ErrCode, "E_"))
	default:
		return "startup_failed"
	}
}

// ----------------------------------------------------------------- health ---

func (s *Supervisor) setRenewOK(ok bool) {
	s.mu.Lock()
	s.lastRenewOK = ok
	s.mu.Unlock()
}

func (s *Supervisor) appliedGrant() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grantApplied
}

// ackDelivered clears the queued acknowledgement once the control-plane's own
// answer proves it landed. The renew response carries acked_epoch as the server
// recorded it, so the worker drops the ack only on the server's word rather
// than on "I sent it" — a renew whose request was written and whose response
// was lost leaves the ack queued, and the next renew re-sends it.
func (s *Supervisor) ackDelivered(recorded int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingAck > 0 && recorded >= s.pendingAck {
		s.pendingAck = 0
	}
}

func (s *Supervisor) writeHealth() {
	s.noteQuotaTrips()
	s.mu.Lock()
	h := Health{
		Epoch:             s.Spec.FenceEpoch,
		LeaseExpiresAt:    s.deadline.WallExpiry(),
		LastRenewOK:       s.lastRenewOK,
		PendingBlocks:     s.vol.PendingBlocks(),
		LastBarrierAt:     s.lastBarrier.BarrierAt,
		UsedBytes:         s.lastUsage.Bytes,
		UsedInodes:        s.lastUsage.Inodes,
		GrantEpochApplied: s.grantApplied,
		QuotaExhausted:    s.quotaExhausted,
		Fenced:            s.fenced,
		StagingBacklogCap: s.backlogCap,
		ReplicationFailed: !s.replFailedSince.IsZero(),
	}
	h.ProjectedDrainSeconds = s.drain.Project(h.PendingBlocks).Seconds()
	h.DrainRateBlocksPerSecond = s.drain.RatePerSecond()
	h.DrainSamples = s.drain.Samples()
	if !s.lastBarrier.BarrierAt.IsZero() {
		h.ReplicaLagMs = s.now().Sub(s.lastBarrier.BarrierAt).Milliseconds()
	}
	s.mu.Unlock()
	if w := s.Deps.Credentials; w != nil {
		h.CredentialRefreshFailed = w.Verdict() != CredentialOK
		h.CredentialGeneration = w.Generation()
	}
	if err := writeJSONAtomic(s.Paths.HealthPath(), h); err != nil {
		s.log("health_write_failed", "error", err.Error())
	}
}
