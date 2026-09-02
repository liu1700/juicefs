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

// Replicator is the metadata-replica half: Litestream, driven as a child
// process (see litestream.go for why exec beats the library here).
type Replicator interface {
	// Restore materialises the metadata database from the replica. It reports
	// ErrReplicaEmpty when the prefix holds no generation at all, which is
	// the first-boot signal rather than a failure.
	Restore(ctx context.Context, timestamp time.Time) error
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
}
