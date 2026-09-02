# Restore verification and restore-time repair (`pkg/plori/restore`)

`pkg/plori/restore` is the leaf package behind two steps of the `juicefs
plori-mount` startup chain: proving a restored metadata database is sound and
safe to replicate onward, and repairing the data-plane damage an unclean
generation leaves behind.

It never mounts, never holds a lease, never talks to the control plane and
never runs Litestream. `pkg/plori/mount` owns the lifecycle and calls in here
through two `Volume` methods that `cmd/plori_mount.go` implements. Everything
in the package is behind the `plori` build tag, so no other JuiceFS build
compiles it.

## Why Litestream is not imported here

Restore itself runs through the pinned Litestream **binary**
(`pkg/plori/mount/litestream.go`), not the library.

Litestream v0.5.17 opens the database it replicates with `modernc.org/sqlite`
(`db.go:1049`, plus `sqlite.FileControl` at `db.go:1009` for checkpoint
control). JuiceFS opens that same file with `mattn/go-sqlite3`. Linking both
would put two independent SQLite library instances on one database file inside
one process, which SQLite does not support: POSIX advisory locks are held per
process, so closing any descriptor on the file drops every lock the process
holds on it, and each library instance keeps its own inode and WAL-index
registry, so neither can see the other's locks or shared memory. Two SQLite
builds in two processes is the configuration the locking protocol is designed
for, and it is what the M0 harness measured working.

`modernc.org/sqlite` sitting in `hack/verify_plori_sbom.py`'s `DENIED_PREFIXES`
is that same fact written down as a gate. An earlier draft of this package used
the library for restore only — which is safe in isolation, because nothing has
the database open at that point — and admitting the dependency was measured at
**+6.9 MB of binary (+12.1 %) and +11 modules**, including a second SQLite
implementation that keeps `sqlite3_enable_load_extension`
(`modernc.org/sqlite@v1.49.1 lib/sqlite_linux_amd64.go`) where the audited
engine compiles it out (`sqlite_omit_load_extension` in `PLORI_TAGS`). The
support policy would have had to say the profile links a second SQLite. It does
not, and the gate is unchanged: **this package's dependency delta against
`main` is zero.**

Every check below therefore runs on the audited engine, the same one the mount
uses.

## API

### Metadata gate

```go
func IntegrityCheck(ctx context.Context, dbPath string, quick bool) error
func LoadFormat(ctx context.Context, dbPath string, tablePrefix ...string) (*meta.Format, error)
func CheckReplicable(format *meta.Format) error
func Scrub(f *meta.Format) *meta.Format
func VerifyRestored(ctx context.Context, dbPath string, quick bool, tablePrefix ...string) (*meta.Format, error)
```

`VerifyRestored` is the whole gate in one call, in this order:

1. `PRAGMA integrity_check` over the restored page image, reporting **every**
   line the check produced rather than the first. Litestream's own restore-time
   check proves the LTX chain replays; this proves the image it produced is a
   sound database. `quick` reduces it to `PRAGMA quick_check`, which skips the
   cross-index pass — the right gate for a warm restart, never for a restore.
2. The Format, read straight out of `jfs_setting` rather than through
   `meta.NewClient`, which calls `logger.Fatalf` on a database it cannot use
   and would take the supervisor's process with it. The whole point of these
   checks is to refuse rather than die.
3. `CheckReplicable`, which is two refusals:
   * **`E_FORMAT_CARRIES_CREDENTIALS`** (threat-model F-9). A Format still
     holding `AccessKey`, `SecretKey` or `SessionToken` must never reach a
     replica prefix: the key is bucket-wide and the prefix is readable by
     anything that can read the bucket, so a credential inside it widens the
     blast radius from one mount to the whole store. JuiceFS persists those
     fields by default (`cmd/format.go`), so this is a live regression risk on
     every format path, which is why it is checked against the database rather
     than against the code that wrote it. The message names the field and never
     the value.
   * **`E_VOLUME_TRASH_DISABLED`**. With `trash-days 0`, a restore that lands
     before a delete resurrects metadata whose blocks are already gone and
     leaves the repair nothing to work from (`crash-consistency.md` §7).

Identity is deliberately **not** here. The three-way match needs the MountSpec
and a live object-store handle, and `pkg/plori/mount` owns both
(`Supervisor.identityMatches`): the spec says which volume the mount is for,
the Format says which filesystem the metadata claims to be, and the
`juicefs_uuid` **object** in the data prefix — a store read, not a database
read — says which filesystem owns the data. `Format.Name` is the data prefix
`agents/<vid>`, because the S3 backend ignores any path beyond the bucket, so
the spec compares it as such. A second implementation of that match in this
package would be a second thing to keep in step.

### Restore-time repair

```go
type BlockRef struct { Inode meta.Ino; Path string; Slice uint64; Chunk uint32; Key string; Size int; Offset uint64 }

func ScanMissingBlocks(ctx context.Context, m SliceScanner, store BlockHeader, opt ScanOptions) (*ScanReport, error)
func DeletedInos(ctx meta.Context, m meta.Meta) (map[meta.Ino]bool, error)
func Quarantine(ctx context.Context, m Quarantiner, records []BlockRef, mode QuarantineMode, format *meta.Format) (*QuarantineReport, error)
```

`SliceScanner`, `BlockHeader` and `Quarantiner` are three-method slices of
`meta.Meta` and `object.ObjectStorage`, so the repair is unit-testable without
FUSE, Redis or a network object store.

`DeletedInos` takes the full `meta.Meta` rather than an interface because
`ScanDeletedObject`'s callback types are unexported and cannot appear in an
interface declared outside `pkg/meta`; the body is the same call
`cmd/fsck.go:164-170` makes.

## Repair algorithm (`crash-consistency.md` §7 d3)

A generation that did not write the clean marker died before its ordered stop
finished, so the metadata Litestream replicated can reference blocks the
writeback cache never uploaded. Those files stat fine and read `EIO` — the
crux the M0 harness reproduced. `juicefs fsck` detects the condition
(`cmd/fsck.go:172-245`) but its `--repair` only fixes directories
(`cmd/fsck.go:59-76`), so the repair action itself lives here. This package
never shells out to `juicefs fsck`; it mirrors the traversal.

1. **Enumerate.** `m.ScanSlices` yields every `(inode, slice)` the metadata
   references. Slices below `ScanOptions.MinSliceID` and inodes in
   `ScanOptions.SkipInos` are dropped.
2. **HEAD.** For each slice, every block key is built exactly as
   `cmd/fsck.go:229-235` builds it — `<id>_<index>_<size>`, under
   `%02X/%d/` when `HashPrefix` is set and `%d/%d/` otherwise — and HEADed
   through a `chunks/`-prefixed store handle, in a bounded worker pool
   (8 by default).
3. **Fail closed.** A HEAD failure that is not `os.ErrNotExist` ends the scan
   with a retryable error. `juicefs fsck` logs such a failure and carries on;
   a repair decision taken from a scan with holes in it would truncate healthy
   files.
4. **Report.** Missing blocks come back sorted by `(inode, slice, chunk)`, each
   with the file path resolved once per inode, so two runs over the same damage
   produce the same report.
5. **Quarantine.** `ModeTruncate` cuts each damaged file at the first byte a
   missing block would have supplied; `ModeMarkOnly` changes no file content.
   Both write the marker. Neither deletes anything.

### Why a full scan by default

`MinSliceID` is a watermark, not a correctness argument. JuiceFS has no
per-slice transaction id, so the closest available proxy is the slice id, which
comes from a monotonic counter in the metadata engine: "allocated after the
durable point" implies "id at or above the id in use then". A lower id whose
block upload was still in flight is missed. The default is therefore zero, a
full scan — PLO-316 wave 2 measured 870 ms, 12 LIST calls and 34 MiB on an
11k-object volume, against roughly 15 times that for the path-scoped form, so
the affordable variant is also the only complete one.

### The truncation boundary

`meta.Read` returns each chunk as an ordered, gap-free cover
(`pkg/meta/slice.go:144-153`), so a running position over `Len` gives the file
offset of every slice entry, and `Off` is that entry's start inside the slice
object. The first byte of block `b` the entry actually references is
`max(b*BlockSize, Off)`, at file offset
`indx*ChunkSize + pos + max(b*BlockSize, Off) - Off`.

If that offset cannot be located the entry degrades to mark-only rather than
guessing. An over-eager truncation destroys data that is still there, which is
strictly worse than the damage being repaired.

### Why an xattr, and why the report is returned rather than written

The marker is the extended attribute `trusted.plori.quarantine`, holding
`{"code":"E_BLOCK_MISSING_AFTER_RESTORE","at":…,"truncated_to":…,"blocks":[…]}`.

The `trusted.` namespace is doing real work. The supervisor sets the attribute
through the metadata engine, which performs no namespace check, while an Agent
reading or clearing it goes through FUSE, where the kernel restricts
`trusted.*` to `CAP_SYS_ADMIN` (`fs/xattr.c`). The marker is therefore writable
by us and read-only to the tenant without a second enforcement point. It also
travels with the file across renames.

The alternative — a `.plori-quarantine/` manifest inside the volume — was
rejected on three counts: it needs data-plane writes during exactly the window
where the data plane is known to be damaged, it lands in the tenant's
namespace, and the tenant can delete it. `Quarantine` instead **returns** a
`QuarantineReport`, and the supervisor persists it beside the mount state and
reports it onward. The durable operator record belongs outside the tenant's
filesystem.

## What the supervisor calls, and when

`pkg/plori/mount/runtime.go` declares both seams on `Volume`;
`cmd/plori_mount.go` implements them over this package.

| Startup step | Call | On failure |
|---|---|---|
| after `FS.Open`, before identity | `Volume.IntegrityCheck` → `VerifyRestored` | exit 67, `E_RESTORE_INTEGRITY` (or `E_FORMAT_CARRIES_CREDENTIALS` / `E_VOLUME_TRASH_DISABLED`) |
| after `PurgeSessions`, before `Replicator.Start` | `Volume.RepairAfterRestore` → `ScanMissingBlocks` + `Quarantine(truncate)` | exit 67, `E_RESTORED_TO_BARRIER` |

The repair runs **only** when the previous generation left no clean marker, and
never on a volume this process formatted. Running it before replication starts
puts the repair's metadata writes inside the first transaction the epoch
replicates, so no Agent ever observes the stat-ok/read-`EIO` file, and the
repaired state is what a later restore comes back to.

A repair failure refuses the mount. Serving a filesystem whose damage was
neither bounded nor recorded is worse than refusing to mount it.

## What the neighbours own

* **PLO-321** owns restore itself (`Litestream.Restore`, over the pinned
  binary), the source-prefix discovery (`Fencer.PriorMetaPrefix` lists
  `agents-meta/<vid>/` and picks the newest `g<N>/` below the current epoch
  that holds more than a `fence` object), the identity match, the lease and the
  ordered stop.
* **PLO-326** persists the recovery anchor: `T_before`, the pre-barrier wall
  clock, plus the replica TXID, in `<state-dir>/durable-point.json` and at the
  control plane. `Restore` prefers a TXID because it is clock-free; the
  timestamp fallback is safe and one-sided, since Litestream selects LTX files
  with `CreatedAt < T` (`replica.go:1535`, `:1673`) and `CreatedAt` is stamped
  after the WAL drain (`db.go:2141` after `db.go:2080`), so a restore lands at
  or behind `T`, never ahead of it.
* **PLO-348**'s restore drill asserts the round trip: replicate a populated
  volume, restore it, `VerifyRestored` it, mount it, read the same bytes back;
  then delete one block behind JuiceFS's back and assert that
  `ScanMissingBlocks` finds exactly that block, that `Quarantine(truncate)`
  leaves the file readable up to the boundary, and that the file carries
  `E_BLOCK_MISSING_AFTER_RESTORE`.

## Tests

`make test.plori.meta` runs `./pkg/meta/` and `./pkg/plori/...` under
`PLORI_TAGS`. This package's tests need no Redis and no network: they build a
real volume through the JuiceFS SDK write path onto a `file` object store,
because a Mac cannot FUSE-mount a volume and the SDK path writes the same
metadata and the same block objects a mount would. The technique follows
`bench/storage/offline-reader/fixture.go` on `project/per-agent-juicefs`.

## Not implemented

* **Litestream retention for retired volumes.** Snapshot interval, L0 retention
  and the compaction levels are set in `pkg/plori/mount/litestream.go`, but
  nothing prunes an abandoned epoch's metadata prefix after its volume is
  retired. Owner: PLO-320, still open.
* **A repair record at the control plane.** `QuarantineReport` is returned and
  logged; no endpoint stores it, so an operator asking "which files did we cut"
  has only the worker's stderr and the xattrs.
