---
title: The plori-mount lifecycle supervisor
sidebar_position: 9
---

`juicefs plori-mount` is the Plori distribution's mount entrypoint. One
foreground process owns exactly one Agent volume for its whole lifetime: it
claims the writer epoch in the object store, restores the metadata replica,
proves the mount opens the filesystem it was told to open, holds the
control-plane lease, and runs the ordered durability shutdown when it is asked
to stop.

This command adds the Plori-specific lifecycle. It is compiled only into builds
with the `plori` build tag.

## Invocation

```
juicefs plori-mount \
  --spec-file          /run/plori-mount/<pod-uid>/spec.json \
  --mount-point        /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<vol>/mount \
  --state-dir          /var/lib/plori-mount/<storage_volume_id> \
  --cache-dir          /var/lib/plori-mount/<storage_volume_id>/cache \
  --control-plane-url  https://<control-plane>/ \
  --token-file         /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~projected/<vol>/token \
  [--litestream-bin    /usr/local/bin/litestream] \
  [--replicator        /var/lib/plori-mount/.replicator/litestream.sock]
```

`--replicator` picks the replication topology (PLO-366).

With it, the metadata replica is driven by the **node-level** `litestream
replicate` the CSI plugin runs and supervises: this worker registers its own
database over that control socket, with its own per-epoch replica prefix, and
does not start a continuous Litestream process of its own.

Without it, the worker starts its own `litestream replicate` child.

When the worker runs its own `litestream replicate` child, it restarts that
child at once when it has exited, and after 3 consecutive failed probes when it
is still running (SIGTERM first, SIGKILL if it has not exited within 5 s).
Each probe waits for local WAL processing and remote replication, bounded by
5 s; a responsive child whose upload stalls therefore fails its probe. An idle,
caught-up replica remains healthy. A
replacement is never killed while it is still inside its own 30 s wait for the
control socket. Replication that has not recovered within the 30 s replication
recovery window, capped by the lease stop instant, stops the mount with exit 69.

`litestream restore` is a one-shot process on both paths. It runs before the
database exists, reads from a different prefix than the one this generation
writes to. It does not remain running after restore.

One thing the shared daemon cannot do: stop replicating a database **without** a
final sync. `POST /unregister` and `POST /stop` both reach `db.Close`, which
syncs. The out-of-band fence path therefore unregisters on the shortest budget
the API accepts rather than killing a process. What still holds is the rest of
the protocol: no `clean` marker is written, so the next generation takes the
unconditional `fsck` and the restore-time repair, and a successor with a
recorded durable point requests that restore anchor. If compaction prevents an
exact TXID restore, recovery can include later transactions. Restore-time
repair checks their block references. See [Recovery](#recovery).

The object credential comes from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
in the worker's own environment. The MountSpec carries none, and a spec whose
`credential_source` is anything other than `node_secret` is refused rather than
served from some other source.

The plugin passes exactly `PATH`, `GOMEMLIMIT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and optionally `PLORI_MOUNT_OPTIONS`. Missing object
credentials are exit 68.

## The MountSpec

The worker decodes the control-plane's `storagespec.MountSpec` with
`DisallowUnknownFields`. Its wire types live in `pkg/plori/mountspec`, which
does not require the `plori` build tag. `pkg/plori/mount` re-exports aliases
such as `MountSpec` and `LoadSpec`.

The control-plane owns this wire contract. The plori-runtime storage-worker
checks compatibility against generated fixtures in
`services/control-plane/internal/storagespec/testdata/*.golden.json`.

Two fields drive the first boot:

| field | meaning |
|---|---|
| `format` | everything `juicefs format` needs for this volume: `volume_id`, `bucket` (`<endpoint>/<bucket>`, no deeper), `data_prefix`, `meta_prefix` (the metadata ROOT, not this writer's epoch inside it), `trash_days`, `capacity_bytes`, `inodes`, `grant_epoch`, `expected_uuid` |
| `may_format` | the authorisation to run `juicefs format`, true exactly when the volume has never been formatted |

`may_format` grants explicit formatting authority. An empty `expected_uuid`
does not grant that authority. The worker refuses inconsistent values repeated
in `format` and the enclosing spec with exit 64. For example, both bucket
values must agree. Block size, compression, and the storage driver are profile
constants, not wire fields.

## Mount options

`mount_options` is a closed vocabulary, not a list of `juicefs` flags. The two
sides version independently, so the worker understands a vocabulary rather than
a command line.

| key | default | effect |
|---|---|---|
| `writeback` | on | the crash-consistency protocol is writeback plus barrier |
| `allow_other` | off | passed through the `-o` string, which sets it at any uid; the upstream default only sets it for uid 0 |
| `buffer_size=` | `32` | MiB; the chunk store raises anything smaller anyway |
| `heartbeat=` | `300` | seconds or a Go duration |
| `barrier_interval=` | `60` | seconds or a Go duration |
| `litestream_sync=` | `1s` | replica sync interval |
| `gomemlimit=` | unset | consumed by the plugin, which exports `GOMEMLIMIT`; the Go runtime reads it directly |

The worker logs and ignores an unrecognised mount-option key. It refuses an
unknown top-level spec field because that field can describe authority the
worker does not support.

`PLORI_MOUNT_OPTIONS` replaces the whole option list. It does not merge with
the spec's options.

## Exit codes

The plugin maps each of these to a NodePublish error and a kubelet event, so a
code never gets reused for a different meaning.

| code | meaning | plugin action |
|---|---|---|
| 0 | clean stop after SIGTERM: fenced, barrier, unmount, final sync, lease released. Also an abandoned startup (`E_STOPPED_BEFORE_MOUNT`). Nothing was published and no `ready` file exists, so a publish waiting for it still fails | normal |
| 64 | spec invalid, unsupported `credential_source`, or an unknown field the worker must not ignore | fail publish, no retry |
| 65 | identity mismatch (Format Name/UUID against the spec or the `juicefs_uuid` object, or the control-plane refused the format acknowledgement) | fail publish, no retry; the control-plane is told via `/lease/release reason=identity_mismatch` |
| 66 | lease lost: renew returned `stale_epoch`/`lease_held`, the deadline passed, the fence marker was already held, or the FUSE session ended unexpectedly. `E_FENCED_OUT_OF_BAND` means the epoch was revoked. The worker stopped without a barrier or final sync | unpublish; the abnormal-exit guard cancels the run |
| 67 | restore failed: replica missing, corrupt, or failed its integrity check | fail publish; retryable only if the error JSON says so |
| 68 | object store unreachable or credential rejected at startup | fail publish, retryable |
| 69 | durability incomplete, lease still released. `E_BARRIER_INCOMPLETE` identifies a barrier or writeback-drain failure. `E_REPLICATION_FAILED` identifies a final-sync failure or replication that did not recover within the 30 s replication recovery window, capped by the lease stop instant | unpublish; surface as a typed event |
| 70 | `.control` would be Agent-writable, the cache dir holds another tenant's staging, or trash-days is 0 | fail publish, no retry |

The last line on stderr is a single JSON object with a typed `error` field
(`E_STOPPED_BEFORE_MOUNT`, `E_SPEC_INVALID`, `E_IDENTITY_MISMATCH`,
`E_FENCE_MARKER_HELD`, `E_FENCED_OUT_OF_BAND`, `E_RESTORE_FAILED`,
`E_RESTORE_INTEGRITY`, `E_RESTORED_TO_BARRIER`, `E_BARRIER_INCOMPLETE`,
`E_REPLICATION_FAILED`, `E_VOLUME_TRASH_DISABLED`,
`E_CACHE_DIR_TENANT_MISMATCH`, `E_CONTROL_FILE_AGENT_WRITABLE`,
`E_OBJECT_STORE_UNREACHABLE`, `E_LEASE_LOST`). It is assembled from a closed
field set rather than from a formatted struct, so it can be republished into a
Pod event without leaking anything. Several identifiers share one exit code on
purpose: the code decides what the plugin DOES with the mount, the identifier
tells an operator which of the conditions behind it happened, and adding a
number for each would change the plugin's table every time a condition is split.

## Startup

1. Parse and validate `--spec-file`. Unknown JSON fields are refused: a field a
   newer control-plane added and this worker silently dropped is exactly the
   downgrade the closed credential vocabulary exists to prevent.
2. Claim the epoch's fence marker with `If-None-Match: *`. This happens before
   the restore and before any LTX object is written, so a second writer that
   somehow reached the epoch fails its own first write. A 412 is exit 66.
3. Find the generation to restore from and restore it with Litestream. The
   metadata root is partitioned per writer epoch, so this epoch's own prefix is
   empty by construction: the worker lists `agents-meta/<vid>/`, takes the
   newest `g<N>/` below its own epoch that holds more than a fence marker, and
   restores from that while replicating forward into its own. A prefix holding
   only `fence` is a writer that claimed an epoch and died before replicating,
   and is not a restorable generation. An empty result means "new volume" only
   on migration generation 1, in state `formatted` or `allocating`, and only
   when the spec's `may_format` grants it. Anywhere else an empty replica means
   the replica was lost, and formatting there would replace a filesystem with an
   empty one.
4. Run `PRAGMA integrity_check` on the restored database. Litestream's own
   restore-time check proves the LTX chain replays; this proves the page image
   it produced is intact.
5. Match identity three ways: the spec, the restored `Format`, and the
   `juicefs_uuid` object under the data prefix. Two of three agreeing is
   exactly the state a swapped replica produces, so all three are required.
   `--force` does not exist here.
6. Refuse (exit 70) on trash-days 0, on a missing `.control` uid gate, or on a
   cache directory holding another volume's staged blocks.
7. Delete every session recorded in the restored metadata. At `--heartbeat 300`
   the previous writer's row does not expire for 25 minutes, and until it does
   it holds POSIX locks and sustained inodes on behalf of a writer the lease
   has already replaced.
8. Start replication and mount FUSE in this process.
9. If the spec says `may_format`, `POST /format-ack` with the `Format.UUID` the
   identity match just proved. That call is what records the UUID and moves a
   generation-1 volume to `active`; until it lands the control-plane still
   believes the volume is being allocated and routes the Agent's Files panel to
   the other storage plane, while this mount is its filesystem (PLO-420). The
   trigger is `may_format` rather than "this process formatted", because a
   worker that formatted, seeded its replica and died before acking comes back
   to a replica that restores and never formats again. A refused ack refuses the
   mount: exit 65, or 66 when the refusal is a fence.
10. Write `<state-dir>/ready` once the mount is in the process's mount table and
    the root inode answers. Everything above has to be true before this file
    exists, because the plugin publishes the volume the moment it appears.

A SIGTERM before step 10 cancels startup. The worker checks cancellation before
the fence claim, after restore, before replication, and after the seed.
It aborts replication, closes the database, and releases the lease with reason
`stopped_before_mount`. It exits 0 with `E_STOPPED_BEFORE_MOUNT`.
An abandoned startup writes no `clean` marker. Its successor sets aside the
interrupted restore instead of adopting it.

If the mount wait fails, the worker ends and joins the FUSE session before
unmounting or closing the volume. The exit line reports the session result.

## The writer lease

`lease_expires_at` is converted to this process's monotonic clock once, at
receipt. Later wall-clock readings do not recompute that deadline.
New writes stop at `expiry - write_stop_margin`. A wall-clock step
larger than one second relative to the monotonic clock is itself treated as a
fence trip.

`stale_epoch` or `lease_held` on renew is terminal. It is never retried,
because a retry is the fenced writer still believing it owns the volume. On a
fence trip the worker revokes its own write permission, runs as much of the
ordered stop as the remaining lease allows, and exits 66.

When the control-plane is simply unreachable, nothing arrives to move the
deadline forward, so the worker stops itself at the margin rather than writing
until someone tells it to stop.

## Shutdown

SIGTERM runs, in order: fence new operations, run the remote durability
barrier, unmount and close SQLite, force a final replica sync, stop the
replicator, report the durable point and the final usage, release the lease.
The whole sequence is bounded by what is left of the lease, because a barrier
that outlives its authority is the fault the fencing design exists to prevent.
If the bound is exhausted, the worker exits 69 and reports incomplete durability.
It still releases the lease. Unreplicated metadata or pending writeback data
can be lost.

The durable point recorded before each barrier is `T_before`, the wall clock
captured *before* the barrier ran. The barrier's own completion timestamp is
not a safe restore point, and the writeback fence counter is a per-process
in-memory sequence that means nothing across restarts; neither is ever
persisted as the anchor.

`<state-dir>/clean` is written as the last act of a clean stop and removed at
the start of every run, so its absence is a reliable signal that the previous
generation died mid-flight.

## Files in the state directory

| file | written | contents |
|---|---|---|
| `meta.db` | restore or format | the SQLite filesystem metadata |
| `litestream.yml` | startup, 0600 | replication config; never contains a credential |
| `litestream.sock` | replication start | Litestream's control socket |
| `ready` | after the mount serves | `{"epoch", "mounted_at", "volume"}` |
| `health.json` | every 10 s and on every renew | `{"epoch", "lease_expires_at", "last_renew_ok", "replica_lag_ms", "pending_blocks", "last_barrier_at", "used_bytes", "used_inodes", "grant_epoch_applied", "fenced"}` |
| `durable-point.json` | after every barrier | the `T_before` anchor plus the replica TXID |
| `clean` | after a clean stop | the timestamp of the stop |

The directory is 0700 and lives outside the Agent's bind mount.

## Defaults

| knob | default | why |
|---|---|---|
| `--backup-meta` | `0` | Litestream is the metadata backup, and the hourly dump was one of the two idle object writers |
| `--no-usage-report` | on | the mount is not a telemetry client |
| `--metrics` | empty | no public TCP metrics listener. In-Pod writers expose private `metrics.sock` |
| Litestream compaction | L1 10 m, L2 1 h, L3 6 h | snapshot interval 24 h and L0 retention 30 m |

These defaults are fixed in the profile. The mount-options table lists the
supported tuning options.

## Why Litestream runs as a child process

Litestream v0.5.17 opens the database it replicates with `modernc.org/sqlite`,
while JuiceFS opens the same file with `mattn/go-sqlite3`. Linking both into
one binary would put two independent SQLite library instances on one database
file inside one process, which SQLite does not support: POSIX advisory locks
are held per process, so closing any descriptor on the file drops every lock
the process holds, and each instance keeps its own inode and WAL-index registry
and cannot see the other's state. Two SQLite builds in two processes is what
the locking protocol is designed for.

`DB.SyncAndWait` is available as `litestream sync -wait` over
the control socket, a single SIGTERM makes `replicate` run its own shutdown
sync, and restore takes a TXID or a timestamp on the command line. Separate
processes also keep the crash domains apart.

A node-level Litestream process can serve multiple databases through explicit
registration. Select this topology with `--replicator`.

## Volume ceiling admission

The admission wrapper attaches an in-memory volume reservation to each metadata
call (`pkg/meta/volume_reservation.go`). The ceiling check reserves space and
inodes before admission. Counter updates replace the reservation with committed
usage. The call releases unused reservations before it returns or waits for a
larger grant. Concurrent calls cannot reuse reserved capacity. Reservations are
not persisted or reported as usage.

Clone reserves its source usage during preflight. Each clone transaction charges
that reservation before commit. A refused charge returns `ENOSPC` without
retrying the clone, because part of the clone can already exist.

The first unlink, rmdir, or rename in an hour can require a trash bucket.
Creating that bucket reserves 4 KiB and one inode. At a full grant, the call
waits for admission. It does not bypass trash. A recursive remove that is
refused partway waits for admission, then removes the remaining entries.
Internal cancellation stops sibling work for that attempt. Caller cancellation
ends the call.

Admission waits use the metadata context's `Canceled()` predicate when available.
The FUSE context has a nil `Done` channel and always returns `EINTR` from `Err`,
including before cancellation. The wait polls `Canceled()` every 100 ms.
Ordinary contexts retain their `Done` and `Err` cancellation behavior.

SQL and KV serialize heartbeat counter refresh with the pending-delta flush.
The single-writer Redis admission client uses the startup counter baseline plus
its own committed deltas. Heartbeat refresh does not overwrite them.
This prevents double counting a remote commit and its local update, including
deletions. Other writers and online counter repair are outside this contract.
Restart the sole client after offline counter repair.

The ceiling applies per metadata client on each engine. This profile requires
one writer for the volume. The ceiling uses logical 4 KiB accounting.
It does not limit physical object bytes.

The wrapper enables single-writer accounting before `NewSession` starts
background work. Grant updates use `PloriApplyGrant` to update the metadata
ceiling and the running client.

## Workspace gateway Litestream metrics

With `--workspace-gateway` only, the replicate config sets `addr:
"127.0.0.1:9909"`. Restore configs and other workers keep `addr: ""`.
An M1 worker runs in the node's network namespace. The listener binds only to
loopback. Pinned Litestream also serves `/debug/pprof` there. Processes in the
gateway's network namespace can reach it.

Litestream continues running if its background listener fails to bind.
The port alone does not identify the child.

The private `metrics.sock` exports `juicefs_plori_litestream_metrics_child`.
This gauge reports the supervised child's start sequence only while that child
uniquely owns the TCP listener. Otherwise, it reports `0`. This includes child
startup, shutdown, restart, unreadable `/proc`, and a listener owned by another
process.

The writer resolves the child in `/proc` once per start, before the child can
be reaped. It uses the parent PID and innermost `NSpid`. Each scrape rechecks
the child's kernel start time and socket ownership.

Read the identity gauge before and after a Litestream scrape. Accept the sample
only when both identity values are equal and nonzero.

## Recovery

The supervisor calls `Volume.RepairAfterRestore` after an unclean generation,
between session purge and replication startup. See
[`plori_restore.md`](plori_restore.md) for missing-block repair.

The MountSpec carries `durable_point` and `restore_from_prefix` when the
control-plane has a recorded durable point. Restore prefers its replica TXID,
then its timestamp. Without either anchor, it restores the latest transaction.
Without an explicit prefix, `PriorMetaPrefix` selects the prior generation.

Compaction can make an exact TXID unreachable. In that case, restore tries the
nearest available transaction boundary at or after the TXID. If no boundary can
be selected, or that boundary is also unreachable, it tries the latest
transaction. Other restore failures stop recovery. Subsequent repair checks for
missing blocks and quarantines affected files. A forward restore can include
transactions after the recorded durable point.

## Lifecycle limits

* **Litestream retention policy.** Snapshot interval, L0 retention and the
  compaction levels are set, but nothing prunes an abandoned epoch's metadata
  prefix after its volume is retired. Owner: PLO-320.
* **Restart supervision.** The Plori worker does not use JuiceFS's child
  supervisor. An unexpected FUSE session exit produces a nonzero worker exit.
  The plugin controls the next action. Owner: PLO-366.

## Tests

`pkg/plori/mount` unit-tests the whole state machine against fakes: spec
refusals and their exit codes, the monotonic deadline arithmetic, the
three-way identity match, the fence marker's 412, the format gating, the
ordered shutdown, and the rendered Litestream config. The fence-marker test
drives a real AWS SDK client against an in-process shim that honours
`If-None-Match: *`.

`hack/plori-mount-e2e/run.sh` defines an end-to-end check in the fork's
`plori` workflow, which has fuse3 and a pinned MinIO. It formats a volume,
mounts it, writes a file, stops with SIGTERM and requires exit 0, then restores
the replica into a fresh state directory under a new writer epoch and reads the
same bytes back. This describes the check, not a result for a particular build.

## Workspace control metrics

The gateway writer also exposes fixed control-call observations on its private
metrics socket. `juicefs_plori_control_calls_total` and
`juicefs_plori_control_duration_seconds_total` cover that writer's Litestream
`sync` calls through response-body completion. Workspace `barrier` and `clone`
execution use `juicefs_plori_workspace_operations_total` and
`juicefs_plori_workspace_operation_duration_seconds_total`; queue time is
excluded. Outcomes are `ok`, `deadline`, `canceled`, `refused`, and `other`.
`juicefs_plori_control_metrics_ready` is 1 only after all finite series are
registered. An absent series or sentinel is unavailable, not zero. These
observations do not establish a durability receipt or authorize readiness.

The producer is optional and enabled for gateway writers. A writer without it
does not provide these observations. The fork counts each actual execution.
A runtime receipt-cache replay that does not call the fork adds no execution.
For `sync`, `ok` follows the caller's existing response checks. A nonwaiting
probe checks HTTP status only. It does not validate the response body as a
durability receipt.

## Quota refusal and interrupts

A volume-ceiling refusal waits for a larger grant for at most three lease-renew
intervals. Once growth is denied, subsequent refusals return `ENOSPC` immediately
while still requesting growth. A closed FUSE request cancel channel returns
`EINTR`; `fuseContext.Err()` alone does not indicate an interrupt. Quota admission
reads this channel through `PloriInterrupt()`; upstream `Done()` behavior is
unchanged. Other contexts use `Done()` for admission cancellation; metadata
contexts without `PloriInterrupt()` also retain the `Canceled()` polling fallback.

The per-mount registry exports `juicefs_plori_quota_trips_total{outcome}`.
Each completed `Supervisor.Admit` call increments `admitted` for a grant received
in time, `refused` for `ENOSPC`, or `interrupted` for cancellation returning
`EINTR`. A fenced mount returns `EROFS` and does not increment these outcomes.
The registry wrapper adds the same constant labels as other JuiceFS metrics.

The quota regression uses a local SQLite volume and a real FUSE mount. It needs
Linux FUSE access, `fusermount`/`fusermount3`, and Python 3; it needs no S3 service
or Litestream. This foreground command is suitable for a FUSE-enabled CI runner:

```sh
PLORI_QUOTA_FUSE_TEST=1 go test -count=1 -timeout 2m \
  -tags "$(make -s plori.tags)" ./pkg/fuse -run '^TestPloriQuota' -v
```

Go `os.CreateTemp` and Python `open()` run in bounded subprocesses. The test
requires `ENOSPC` and checks that each caller produces one to nine creates using
`juicefs_fuse_ops_total{method="create"}`. Admission is bounded at 150 ms in this
test, with a separate one-second process-start and scheduling allowance.
