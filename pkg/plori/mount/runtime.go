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
	"time"
)

// Paths are the four directories and files the plugin hands the worker.
type Paths struct {
	SpecFile   string
	MountPoint string
	StateDir   string
	CacheDir   string
	TokenFile  string
}

// MetaPath is the restored SQLite metadata database.
func (p Paths) MetaPath() string { return p.StateDir + "/meta.db" }

// ReadyPath is the readiness file the plugin polls.
func (p Paths) ReadyPath() string { return p.StateDir + "/ready" }

// HealthPath is rewritten every renew tick.
func (p Paths) HealthPath() string { return p.StateDir + "/health.json" }

// CleanStopPath records that the previous generation completed its ordered
// stop. It is written as the last act of a clean shutdown and removed at the
// start of every run, so its absence is a reliable "the previous writer died
// mid-flight" signal rather than a guess.
func (p Paths) CleanStopPath() string { return p.StateDir + "/clean" }

// DurablePointPath persists the pre-barrier wall clock the next generation
// restores to. crash-consistency.md §5: neither LastSuccessfulBarrierUnixMs
// (a completion time, cached_store.go:1302-1303) nor Fence (a per-process
// in-memory sequence, :1122-1141) is a restore anchor, so the anchor is
// written here, by us, before the barrier runs.
func (p Paths) DurablePointPath() string { return p.StateDir + "/durable-point.json" }

// FormatIdentity is what the three-way identity match compares.
type FormatIdentity struct {
	Name      string
	UUID      string
	TrashDays int
	Capacity  uint64
	Inodes    uint64
}

// BarrierResult is one completed `juicefs durability` barrier.
type BarrierResult struct {
	// DurableAt is the pre-barrier wall clock T_before, captured by the
	// caller before the barrier started. Everything written before it is
	// durable once the barrier returns.
	DurableAt time.Time
	// BarrierAt is when the barrier completed.
	BarrierAt time.Time
	// PendingBlocks is what the writeback cache still owed when the barrier
	// finished; zero on success.
	PendingBlocks uint64
}

// RepairReport is one restore-time repair pass over the data plane
// (crash-consistency.md §7 d3). It is logged and reported, never swallowed:
// an Agent whose files were cut short has to be able to find out why.
type RepairReport struct {
	// Scanned is the number of slices the scan considered.
	Scanned int `json:"scanned"`
	// Checked is the number of blocks the scan asked the object store about.
	Checked int `json:"checked"`
	// Missing is the number of blocks the store did not hold.
	Missing int `json:"missing"`
	// Files is the number of inodes affected.
	Files int `json:"files"`
	// Truncated is how many of those were cut back to their last readable
	// byte; the rest carry the marker only.
	Truncated int `json:"truncated"`
	// Elapsed is the wall time of the scan and repair.
	Elapsed time.Duration `json:"elapsed"`
}

// Usage is the volume's consumption as the metadata engine sees it.
//
// TrashBytes/TrashInodes are the part of Bytes/Inodes that a deleted file is still
// holding — a SUBSET of the total, never something to add to it. With `trash-days >= 1`
// an unlink is a rename into `.trash`, so the bytes stay inside the counter the volume
// ceiling is enforced against; the panel's own soft-delete into `/.plori-trash` is a
// rename too. Both are measured together, because to a user "the trash" is one thing
// and emptying it empties both (meta.PloriMeasureTrash).
//
// The breakdown is optional and the flags say why. A report may carry `used_bytes` with
// no trash number at all, and the product then says nothing about the trash rather than
// guessing at it.
type Usage struct {
	Bytes  int64
	Inodes int64
	// TrashKnown is false when the trash walk failed. The two numbers below are then
	// meaningless and are not reported.
	TrashKnown  bool
	TrashBytes  int64
	TrashInodes int64
	// TrashPartial is true when the walk hit its entry budget: the numbers are a floor,
	// not an amount, so nothing may present them as "this much would be freed".
	TrashPartial bool
}

// FS is the JuiceFS half of the supervisor. cmd/plori_mount.go implements it;
// this package never imports the mount stack, so the whole state machine is
// testable without FUSE, Redis or an object store (ADR D4).
type FS interface {
	// Format initialises a brand-new volume, credential-free: the object key
	// is injected in-process at Open time and never reaches the metadata that
	// Litestream replicates (PLO-322 / mountspec.md §5).
	Format(ctx context.Context, spec *MountSpec) error
	// Open loads the restored metadata replica, injects the credential
	// through the storage patch and returns the volume without mounting it.
	// It must not write anything.
	Open(ctx context.Context, spec *MountSpec) (Volume, error)
}

// Volume is one opened, not-yet-mounted JuiceFS filesystem.
type Volume interface {
	// Identity is read out of the restored Format.
	Identity() FormatIdentity
	// IntegrityCheck runs SQLite's own `PRAGMA integrity_check` over the
	// restored database. Litestream's restore-time check verifies the LTX
	// chain; this verifies the page image it produced.
	IntegrityCheck(ctx context.Context) error
	// StoredUUID reads `<data prefix>juicefs_uuid` from the object store. It
	// is the third leg of the identity match: the Format says which volume
	// this metadata claims to be, and this object says which volume actually
	// owns the data prefix (threat-model F-10 / R11).
	StoredUUID(ctx context.Context) (string, error)
	// RepairAfterRestore scans the restored metadata against the object store
	// and quarantines every file that references a block the store does not
	// hold (crash-consistency.md §7 d3). `juicefs fsck` detects this condition
	// but its --repair only fixes directories, so the repair action itself
	// lives in pkg/plori/restore.
	//
	// It runs only after an unclean generation, and only before replication
	// starts, so its metadata writes are part of the first transaction this
	// epoch replicates and no Agent ever sees the stat-ok/read-EIO file.
	RepairAfterRestore(ctx context.Context) (RepairReport, error)
	// PurgeSessions deletes every client session recorded in the restored
	// metadata before this process opens its own (PLO-362). With
	// --heartbeat 300 the previous writer's row does not expire for 25
	// minutes, and until it does it holds POSIX locks and sustained inodes
	// that belong to a writer the lease has already replaced.
	PurgeSessions(ctx context.Context) (int, error)
	// Serve mounts FUSE and blocks until the session ends.
	Serve(ctx context.Context) error
	// AwaitMounted returns once the filesystem is serving: the mount is
	// visible in the process's own mount table and the root inode answers.
	AwaitMounted(ctx context.Context) error
	// Barrier flushes the VFS and runs the remote durability barrier.
	Barrier(ctx context.Context) (BarrierResult, error)
	// PendingBlocks is the cheap, non-blocking writeback status used by
	// health.json.
	PendingBlocks() uint64
	// SetStagingBacklogCap bounds how many blocks may sit staged and not yet
	// uploaded. Above the cap a write is uploaded THROUGH rather than staged:
	// the writer waits for the object store and the backlog stops growing.
	// Nothing is dropped and nothing answers an error.
	//
	// The supervisor moves it as the measured drain rate moves, so the deepest
	// backlog it allows is always one the ordered stop can still drain inside
	// the lease (PLO-383). A cap of 0 is unlimited, which is what a volume
	// gets if the profile constant is ever set to 0.
	SetStagingBacklogCap(blocks int64)
	// Usage reports current consumption.
	Usage(ctx context.Context) (Usage, error)
	// ApplyGrant writes a new quota ceiling into the metadata. It must load a
	// fresh Format from the engine rather than persisting the in-memory one,
	// which carries the injected credential.
	//
	// It takes effect on the next metadata operation: the ceiling is read from
	// an atomic pointer per call, so there is no remount and no window in which
	// half the mount enforces the old number (meta.PloriApplyGrant, proved by
	// meta.TestTheCeilingIsReadPerOperationSoAGrantNeedsNoRemount and
	// vfs.TestPloriGrantAppliesLiveThroughTheVFS).
	ApplyGrant(ctx context.Context, bytes, inodes int64) error
	// QuotaTrips is how many operations the VOLUME ceiling has refused since
	// this process started. It is monotonic, so the supervisor can tell "the
	// grant ran out since the last tick" from "the grant ran out once, an hour
	// ago" — which is the difference between asking the control-plane to grow
	// the grant and asking it once a tick forever (PLO-324).
	//
	// Only the volume ceiling counts. A directory, user or group quota answers
	// EDQUOT and is not a ceiling the account allocator can raise, so growing
	// the grant would not help.
	QuotaTrips() uint64
	// SetWriteExpiry publishes the monotonic instant at which this mount's
	// lease dies. The metadata engine re-reads it before every gated
	// operation, which is the per-submission deadline check threat-model.md
	// :812-815 requires and the one-second ticker never was (PLO-323 F-5).
	SetWriteExpiry(at time.Time)
	// FenceWrites seals the filesystem: after it, every mutating metadata
	// operation answers EROFS, including the slice commit of a handle opened
	// before the call (PLO-323 F-2).
	//
	// It is total, so it is NOT the opening move of an ordered stop — the
	// bounded flush that stop reserves the write-stop margin for commits
	// through the very path this seals (threat-model.md §7.5). The supervisor
	// calls it immediately when the authority is gone out of band, and
	// otherwise only once the mount is detached.
	FenceWrites()
	// Fenced reports whether FenceWrites has been called.
	Fenced() bool
	// Unmount detaches the mount with `umount --flush` semantics: fail-closed
	// if the flush does not complete.
	Unmount(ctx context.Context) error
	// Detach unmounts WITHOUT flushing — a lazy detach. It is the out-of-band
	// fence's way out: a writer that has lost its epoch must not push staged
	// bytes into a data prefix it no longer owns, and `umount --flush` would.
	// It is also the only unmount that can succeed once FenceWrites has sealed
	// the engine, because that flush would answer EROFS (PLO-323 F-1, F-2).
	Detach(ctx context.Context) error
	// Close closes the metadata session and the SQLite database.
	Close() error
}

// ControlPlane is the five-call contract of docs/design/per-agent-juicefs/mountspec.md §3.
//
// The grant conversation rides RenewLease in both directions rather than owning
// routes of its own. Renewal is the only regular round trip a live mount makes,
// it is already authorised as the lease holder, and both halves of the grant
// exchange — "I have applied epoch N" and "I need more" — are facts about the
// holder that are only true while it holds the lease. A separate /grant-ack
// call was a second authorisation of the same claim and a second round trip per
// grant; it is gone (PLO-324).
type ControlPlane interface {
	RenewLease(ctx context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error)
	ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error
	ReportUsage(ctx context.Context, volumeID string, epoch int64, u Usage, at time.Time) error
	ReportDurablePoint(ctx context.Context, volumeID string, epoch int64, r BarrierResult, replicaTxID string) error
	// AckFormat tells the control-plane which filesystem this volume is. It is
	// the only call on this interface that is made once and never again: the
	// control-plane holds no Format.UUID until it lands, and holds it for the
	// life of the volume afterwards (PLO-420).
	AckFormat(ctx context.Context, volumeID string, epoch int64, formatUUID string) (VolumeStateResponse, error)
}

// Replicator is the metadata-replica half.
//
// PLO-320's `pkg/plori/restore` is the intended implementation: it owns
// restore, verification, restore-time missing-block repair and the Litestream
// retention/compaction settings. This interface is the seam it plugs into, and
// litestream.go is the implementation until it lands. One caveat to reconcile
// at merge: that package uses the Litestream LIBRARY, which is safe for a
// sequential restore into a database nothing has opened yet, and is the
// two-SQLite-instances-in-one-process hazard for continuous replication —
// see the note at the top of litestream.go.
// RestoreOptions names the point in the replica's history a restore stops at.
// Both fields come from ONE durable point — never one from each, for the
// reason restoreOrFormat spells out — and the more precise one wins.
//
// The two are not interchangeable, and the difference is a measured one rather
// than a stylistic preference (PLO-396, against the pinned litestream v0.5.17
// and superfly/ltx v0.5.2):
//
//   - A TXID names a transaction. `-txid` takes every LTX file whose MaxTXID is
//     at or below it and refuses the restore outright if the plan cannot reach
//     it exactly (`replica.go:1532`, `:1625`), so it either lands on the
//     recorded point or fails loudly.
//   - A timestamp names a FILE. An LTX file carries one timestamp, stamped when
//     the file is encoded — after every transaction inside it committed
//     (litestream `db.go:2141`) — and `-timestamp` takes a file iff
//     `CreatedAt < T` (`replica.go:1673`). So the last transactions before
//     `T_before` are the ones most likely to sit in a file encoded just after
//     it, and a timestamp restore silently drops them.
//
// Measured: a transaction committed, `T_before` captured, the replica synced —
// the exact shape of runBarrier — then restored both ways from the same
// prefix. `-txid` returned both rows; `-timestamp T_before` returned one. The
// same restore run twice at different sub-second offsets returned different
// databases, because whether the file was encoded before or after `T_before`
// is a race. The TXID restore is not a race.
type RestoreOptions struct {
	// TXID is the replica transaction id recorded with the durable point, in
	// `ltx.TXID.String()` form: exactly 16 lowercase hex digits, which is what
	// `litestream restore -txid` parses (`ltx@v0.5.2/ltx.go:130`). Empty when
	// no durable point carries one.
	TXID string
	// Timestamp is the pre-barrier `T_before`. It is the fallback for a
	// durable point recorded before the fork read a TXID at all, and the zero
	// time means "restore the replica's latest transaction".
	Timestamp time.Time
}

type Replicator interface {
	// Restore materialises the metadata database from `sourcePrefix` at the
	// point `opt` names. It reports ErrReplicaEmpty when that prefix holds no
	// generation at all, which is the first-boot signal rather than a failure.
	Restore(ctx context.Context, sourcePrefix string, opt RestoreOptions) error
	// Start begins continuous replication and returns once the control
	// socket answers.
	Start(ctx context.Context) error
	// SyncAndWait forces a sync and blocks until it completes.
	SyncAndWait(ctx context.Context) error
	// TxID is the replica's current transaction id, reported alongside the
	// durable point.
	TxID(ctx context.Context) (string, error)
	// Stop signals a graceful shutdown (which performs a final sync) and
	// waits for the process to exit.
	Stop(ctx context.Context) error
	// Abort stops replication WITHOUT a final sync. A writer fenced out of
	// band must not push its remaining LTX into the metadata prefix a
	// successor may restore from: the barrier it skipped means that history
	// can reference blocks the store never received (PLO-323 F-1).
	Abort(ctx context.Context) error
}

// ReplicationSupervisor is the half of a Replicator that answers "are you
// still replicating?" and "put it back".
//
// It is a separate interface because it is the answer to a specific failure
// (PLO-411) rather than part of the mount's lifecycle: before it existed, the
// only thing that ever read a replicator's exit was Stop and Abort, so a
// Litestream that died on its own — OOM, a bug, a bad credential rotation —
// left the mount serving writes with no metadata replica and nothing noticed
// until the next barrier, a minute later, if that path checked the error at
// all. ADR B1 makes Litestream the metadata backup, so that window is
// unreplicated writes with a green health file.
//
// Both implementations satisfy it, and the repair each one performs is the
// repair its own failure needs: an exec'd child is restarted, a registration
// with a node-level replicator is re-made.
type ReplicationSupervisor interface {
	// Probe reports whether this worker's database is being replicated right
	// now. It must be cheap enough to run on every health tick and must not
	// block on the object store, so it asks about the LOCAL replication
	// machinery rather than about the replica's contents.
	Probe(ctx context.Context) error
	// Restart puts replication back after Probe failed. The supervisor calls
	// it from its own goroutine, so it never overlaps a barrier or a stop,
	// and never more than once per uninterrupted failure.
	Restart(ctx context.Context) error
}

// Fencer claims the epoch's fence marker in the object store.
type Fencer interface {
	// Claim PUTs the fence marker with If-None-Match: *. It returns
	// ErrFenceMarkerHeld when the store answers 412, which means another
	// writer reached this epoch.
	Claim(ctx context.Context, key string, body []byte) error
	// ReadMarker fetches the marker at `key`, so a worker that got a 412 can
	// tell its own predecessor's claim from a stranger's (PLO-323 F-6).
	ReadMarker(ctx context.Context, key string) (FenceMarker, error)
	// PriorMetaPrefix returns the newest populated metadata prefix under root
	// whose epoch is at or below the given one, or "" when there is none. The
	// metadata root is partitioned per writer epoch, so a fresh epoch starts
	// with an empty prefix of its own and restores from the one before it —
	// but a worker restarted at the SAME epoch restores from its own, which is
	// by then the newest history there is (PLO-323 F-6c).
	PriorMetaPrefix(ctx context.Context, root string, epoch int64) (string, error)
}
