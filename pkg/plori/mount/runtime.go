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
type Usage struct {
	Bytes  int64
	Inodes int64
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
	// Usage reports current consumption.
	Usage(ctx context.Context) (Usage, error)
	// ApplyGrant writes a new quota ceiling into the metadata. It must load a
	// fresh Format from the engine rather than persisting the in-memory one,
	// which carries the injected credential.
	ApplyGrant(ctx context.Context, bytes, inodes int64) error
	// FenceWrites stops the filesystem accepting new writes. It is called
	// before every barrier that must not be outlived by its authority (Q7)
	// and is the first step of the ordered shutdown.
	FenceWrites()
	// Fenced reports whether FenceWrites has been called. The write path
	// consults it before every submission (threat-model §7.2).
	Fenced() bool
	// Unmount detaches the mount with `umount --flush` semantics: fail-closed
	// if the flush does not complete.
	Unmount(ctx context.Context) error
	// Close closes the metadata session and the SQLite database.
	Close() error
}

// ControlPlane is the six-call contract of docs/design/per-agent-juicefs/mountspec.md §3.
type ControlPlane interface {
	RenewLease(ctx context.Context, volumeID string, epoch int64) (LeaseResponse, error)
	ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error
	ReportUsage(ctx context.Context, volumeID string, epoch int64, u Usage, at time.Time) error
	ReportDurablePoint(ctx context.Context, volumeID string, epoch int64, r BarrierResult, replicaTxID string) error
	AckGrant(ctx context.Context, volumeID string, epoch, grantEpoch int64) error
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
type Replicator interface {
	// Restore materialises the metadata database from `sourcePrefix`. It
	// reports ErrReplicaEmpty when that prefix holds no generation at all,
	// which is the first-boot signal rather than a failure.
	Restore(ctx context.Context, sourcePrefix string, timestamp time.Time) error
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
}

// Fencer claims the epoch's fence marker in the object store.
type Fencer interface {
	// Claim PUTs the fence marker with If-None-Match: *. It returns
	// ErrFenceMarkerHeld when the store answers 412, which means another
	// writer reached this epoch.
	Claim(ctx context.Context, key string, body []byte) error
	// PriorMetaPrefix returns the newest populated metadata prefix under root
	// whose epoch is below the given one, or "" when there is none. The
	// metadata root is partitioned per writer epoch, so a new epoch always
	// starts with an empty prefix of its own and has to restore from the one
	// before it.
	PriorMetaPrefix(ctx context.Context, root string, epoch int64) (string, error)
}
