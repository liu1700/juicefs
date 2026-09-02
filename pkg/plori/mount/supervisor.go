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

	mu              sync.Mutex
	lastBarrier     BarrierResult
	lastTxID        string
	grantApplied    int64
	restoredUnclean bool
	lastUsage       Usage
	lastRenewOK     bool
	fenced          bool
	formattedHere   bool
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
	marker := fmt.Sprintf(`{"volume":%q,"epoch":%d,"claimed_at":%q}`,
		s.Spec.StorageVolumeID, s.Spec.FenceEpoch, s.now().UTC().Format(time.RFC3339Nano))
	if err := s.Deps.Fencer.Claim(ctx, s.Spec.FenceMarkerKey, []byte(marker)); err != nil {
		if errors.Is(err, ErrFenceMarkerHeld) {
			return fatalf(CodeFenced, ErrCodeFenceMarkerHeld, false,
				"another writer already claimed epoch %d: %s", s.Spec.FenceEpoch, err)
		}
		return fatalf(CodeObjectStore, ErrCodeObjectStoreUnreachable, true, "claim fence marker: %s", err)
	}
	s.log("fence_marker_claimed", "key", s.Spec.FenceMarkerKey)

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
		s.reportDurablePoint(ctx, BarrierResult{DurableAt: s.now().UTC(), BarrierAt: s.now().UTC()})
	}
	if g := s.Spec.Grant; g.Epoch > g.AckedEpoch {
		s.applyGrant(ctx, g)
	}
	return nil
}

func (s *Supervisor) restoreOrFormat(ctx context.Context) error {
	// There are up to two known durable points, and the newer one wins WHOLE:
	// both the prefix to restore from and the instant to stop at come from the
	// same point, never one from each.
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
		source string
		from   int64
	)
	if dp := s.Spec.DurablePoint; dp != nil {
		anchor, from, source = dp.DurableAt, dp.FenceEpoch, s.Spec.RestoreFromPrefix
		s.log("restore_anchor", "durable_at", anchor, "from_epoch", from, "known_by", "mount_spec")
	}
	if dp, err := ReadDurablePoint(s.Paths.DurablePointPath()); err == nil && dp != nil {
		if dp.Volume == s.Spec.StorageVolumeID && dp.FenceEpoch < s.Spec.FenceEpoch && dp.FenceEpoch > from {
			// Its prefix is populated by construction: the file is written
			// only after a barrier, and a barrier only happens once
			// replication is running under that epoch's prefix.
			anchor, from, source = dp.DurableAt, dp.FenceEpoch, s.Spec.MetaPrefixForEpoch(dp.FenceEpoch)
			s.log("restore_anchor", "durable_at", anchor, "from_epoch", from, "known_by", "state_dir")
		}
	}
	// A previous generation that did not write the clean marker died without
	// finishing its stop, so the restored image is only durable up to its last
	// barrier. crash-consistency.md §7 Rank 1 says the repair for that is a
	// full fsck of the restored metadata against the objects; PLO-320 owes it
	// (PLO-316 wave 2 measured 870 ms / 12 LIST / 34 MiB on 11k objects, so it
	// is affordable unconditionally — never path-scoped, which is 15x worse).
	// Until then the condition is reported, not repaired.
	s.restoredUnclean = !fileExists(s.Paths.CleanStopPath())
	_ = os.Remove(s.Paths.CleanStopPath())

	// The metadata root is partitioned per writer epoch, so this epoch's own
	// prefix is empty by construction and the bytes live under an earlier one.
	// The prefix of the winning durable point above IS that earlier one, which
	// is why the source is settled there rather than derived a second time.
	//
	// With no durable point at all the source is discovered by listing the
	// metadata root and taking the newest prefix below this epoch that holds
	// more than a fence marker. That listing stays: it is the only correct
	// answer for a volume no writer has ever reported a durable point for, and
	// it is what makes `epoch - 1` unnecessary — an epoch that claimed a fence
	// and replicated nothing is skipped by both paths.
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

	err := s.Deps.Replicator.Restore(ctx, source, anchor)
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
	if s.Spec.FormatUUID != "" {
		return fatalf(CodeIdentityMismatch, ErrCodeIdentityMismatch, false,
			"metadata replica is empty but the control-plane recorded format UUID %s", s.Spec.FormatUUID)
	}
	if err := s.Deps.FS.Format(ctx, s.Spec); err != nil {
		return fatalf(CodeRestoreFailed, ErrCodeRestoreFailed, false, "format volume: %s", err)
	}
	s.formattedHere = true
	s.log("formatted", "volume", s.Spec.StorageVolumeID, "trash_days", s.Spec.EffectiveFormat().TrashDays)
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

	ticks := 0
	renewedAt := s.now()
	for {
		select {
		case <-stop:
			s.log("sigterm")
			return s.shutdown(context.Background(), "shutdown")

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
					"wall clock moved %s relative to the monotonic clock; treating as a fence trip", jump))
			}
			if !s.deadline.WriteAllowed(now) {
				s.log("write_stop_margin_reached")
				return s.fenceAndStop(fatalf(CodeFenced, ErrCodeLeaseLost, false,
					"lease deadline reached without a successful renewal"))
			}

		case <-renew.C:
			ticks++
			before := s.now()
			resp, err := s.Deps.CP.RenewLease(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch)
			if err != nil {
				var cpErr *CPError
				if errors.As(err, &cpErr) && cpErr.Fenced() {
					// Terminal by contract: stale_epoch on renew is never
					// retried, because a retry is the fenced writer still
					// believing it holds the volume.
					s.setRenewOK(false)
					return s.fenceAndStop(fatalf(CodeFenced, ErrCodeLeaseLost, false,
						"lease lost: %s", cpErr))
				}
				s.setRenewOK(false)
				s.log("renew_failed", "error", err.Error())
				s.writeHealth()
				continue
			}
			s.setRenewOK(true)
			renewedAt = before
			s.deadline.Update(resp.LeaseExpiresAt, s.Spec.WriteStopMargin.D(), before)
			if resp.Grant.Epoch > s.appliedGrant() {
				s.applyGrant(ctx, resp.Grant)
			}
			if ticks%DefaultUsageReportEvery == 0 {
				s.reportUsage(ctx)
			}
			s.writeHealth()

		case <-health.C:
			s.writeHealth()

		case <-barrier.C:
			s.runBarrier(ctx)
		}
	}
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
	res, err := s.vol.Barrier(bctx)
	if err != nil {
		s.log("barrier_failed", "error", err.Error())
		return
	}
	res.DurableAt = tBefore
	if res.BarrierAt.IsZero() {
		res.BarrierAt = s.now().UTC()
	}
	s.reportDurablePoint(ctx, res)
}

func (s *Supervisor) reportDurablePoint(ctx context.Context, res BarrierResult) {
	txid, err := s.Deps.Replicator.TxID(ctx)
	if err != nil {
		s.log("replica_txid_unavailable", "error", err.Error())
	}
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

func (s *Supervisor) applyGrant(ctx context.Context, g GrantSpec) {
	if err := s.vol.ApplyGrant(ctx, g.Bytes, g.Inodes); err != nil {
		s.log("grant_apply_failed", "epoch", g.Epoch, "error", err.Error())
		return
	}
	if err := s.Deps.CP.AckGrant(ctx, s.Spec.StorageVolumeID, s.Spec.FenceEpoch, g.Epoch); err != nil {
		// The ack is the allocator's signal that the ceiling is locally
		// enforced. Failing to send it is safe — the allocator keeps counting
		// the larger of the two grants — so the local application stands.
		s.log("grant_ack_failed", "epoch", g.Epoch, "error", err.Error())
		return
	}
	s.mu.Lock()
	s.grantApplied = g.Epoch
	s.mu.Unlock()
	s.log("grant_applied", "epoch", g.Epoch, "bytes", g.Bytes, "inodes", g.Inodes)
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

// fenceAndStop is the lease-loss path: stop writes first, then run as much of
// the ordered shutdown as the remaining authority allows.
func (s *Supervisor) fenceAndStop(f *Fatal) *Fatal {
	s.vol.FenceWrites()
	s.mu.Lock()
	s.fenced = true
	s.mu.Unlock()
	if stopErr := s.shutdown(context.Background(), "fenced"); stopErr.Exit == CodeBarrierIncomplete {
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
// full TTL and buys nothing: the data is already lost either way.
func (s *Supervisor) shutdown(ctx context.Context, reason string) *Fatal {
	budget := s.deadline.RemainingLease(s.now())
	if budget < time.Second {
		budget = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	var incomplete error

	// 1. fence new operations
	s.vol.FenceWrites()
	s.mu.Lock()
	s.fenced = true
	s.mu.Unlock()

	// 2 + 3. drain and run the remote durability barrier
	tBefore := s.now().UTC()
	res, err := s.vol.Barrier(ctx)
	if err != nil {
		incomplete = fmt.Errorf("durability barrier: %w", err)
		s.log("shutdown_barrier_failed", "error", err.Error())
	}
	res.DurableAt = tBefore
	if res.BarrierAt.IsZero() {
		res.BarrierAt = s.now().UTC()
	}

	// 4. unmount, then close SQLite. Barrier failure already blocks unmount
	// upstream (cmd/umount.go:120-125); that fail-closed behaviour is kept.
	if err := s.vol.Unmount(ctx); err != nil && incomplete == nil {
		incomplete = fmt.Errorf("unmount: %w", err)
	}
	if err := s.vol.Close(); err != nil && incomplete == nil {
		incomplete = fmt.Errorf("close metadata: %w", err)
	}

	// 5. final replication sync, then stop the replicator (which performs its
	// own shutdown sync).
	if err := s.Deps.Replicator.SyncAndWait(ctx); err != nil && incomplete == nil {
		incomplete = fmt.Errorf("final replica sync: %w", err)
	}
	if err := s.Deps.Replicator.Stop(ctx); err != nil && incomplete == nil {
		incomplete = fmt.Errorf("stop replication: %w", err)
	}

	// 6. report the durable point and the final usage. Both are best effort:
	// they inform the control-plane, they do not make anything durable.
	if incomplete == nil {
		s.reportDurablePoint(context.WithoutCancel(ctx), res)
		s.reportUsage(context.WithoutCancel(ctx))
	}

	// 7. release the writer lease, always.
	s.releaseLease(reason)

	if incomplete != nil {
		return fatalf(CodeBarrierIncomplete, ErrCodeBarrierIncomplete, false,
			"stop did not complete inside the write-stop window: %s", incomplete)
	}
	// The marker is written last and only here, so its absence at the next
	// start is exactly "the previous generation did not finish its stop".
	if err := os.WriteFile(s.Paths.CleanStopPath(), []byte(s.now().UTC().Format(time.RFC3339Nano)+"\n"), 0o600); err != nil {
		s.log("clean_marker_write_failed", "error", err.Error())
	}
	return &Fatal{Exit: CodeOK, Err: errors.New("clean stop")}
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
		return "fenced"
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

func (s *Supervisor) writeHealth() {
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
		Fenced:            s.fenced,
	}
	if !s.lastBarrier.BarrierAt.IsZero() {
		h.ReplicaLagMs = s.now().Sub(s.lastBarrier.BarrierAt).Milliseconds()
	}
	s.mu.Unlock()
	if err := writeJSONAtomic(s.Paths.HealthPath(), h); err != nil {
		s.log("health_write_failed", "error", err.Error())
	}
}
