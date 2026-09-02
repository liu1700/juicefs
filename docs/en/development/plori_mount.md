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

It is not a replacement for `juicefs mount`. Every generic command still works
exactly as it does upstream; this one adds the Plori-specific lifecycle around
them and is compiled only into builds carrying the `plori` build tag.

## Invocation

```
juicefs plori-mount \
  --spec-file          /run/plori-mount/<pod-uid>/spec.json \
  --mount-point        /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<vol>/mount \
  --state-dir          /var/lib/plori-mount/<storage_volume_id> \
  --cache-dir          /var/lib/plori-mount/<storage_volume_id>/cache \
  --control-plane-url  https://<control-plane>/ \
  --token-file         /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~projected/<vol>/token \
  [--litestream-bin    /usr/local/bin/litestream]
```

The object credential comes from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
in the worker's own environment. The MountSpec carries none, and a spec whose
`credential_source` is anything other than `node_secret` is refused rather than
served from some other source.

The plugin passes exactly `PATH`, `GOMEMLIMIT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and optionally `PLORI_MOUNT_OPTIONS`. Missing object
credentials are exit 68.

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
| `gomemlimit=` | — | consumed by the plugin, which exports `GOMEMLIMIT`; the Go runtime reads it directly |

An unrecognised key is logged and ignored. That is the opposite of the rule for
an unknown top-level spec field, and the difference is which side owns the
meaning: an unknown field means the control-plane is describing authority this
worker cannot honour, while an unknown option means it is tuning something this
worker does not have.

`PLORI_MOUNT_OPTIONS` replaces the whole list rather than merging with it,
because a merged list would make the resulting mount a function of two
authorities and neither would be auditable.

## Exit codes

The plugin maps each of these to a NodePublish error and a kubelet event, so a
code never gets reused for a different meaning.

| code | meaning | plugin action |
|---|---|---|
| 0 | clean stop after SIGTERM: fenced, barrier, unmount, final sync, lease released | normal |
| 64 | spec invalid, unsupported `credential_source`, or an unknown field the worker must not ignore | fail publish, no retry |
| 65 | identity mismatch (Format Name/UUID against the spec, or against the `juicefs_uuid` object) | fail publish, no retry; the control-plane is told via `/lease/release reason=identity_mismatch` |
| 66 | lease lost — renew returned `stale_epoch`/`lease_held`, the deadline passed, the fence marker was already held, or the FUSE session ended on its own | unpublish; the abnormal-exit guard cancels the run |
| 67 | restore failed: replica missing, corrupt, or failed its integrity check | fail publish; retryable only if the error JSON says so |
| 68 | object store unreachable or credential rejected at startup | fail publish, retryable |
| 69 | the barrier or the final sync did not finish inside the write-stop window — reported data loss, lease still released | unpublish; surface as a typed event |
| 70 | `.control` would be Agent-writable, the cache dir holds another tenant's staging, or trash-days is 0 | fail publish, no retry |

The last line on stderr is a single JSON object with a typed `error` field
(`E_SPEC_INVALID`, `E_IDENTITY_MISMATCH`, `E_FENCE_MARKER_HELD`,
`E_RESTORE_INTEGRITY`, `E_BARRIER_INCOMPLETE`, `E_VOLUME_TRASH_DISABLED`,
`E_CACHE_DIR_TENANT_MISMATCH`, `E_CONTROL_FILE_AGENT_WRITABLE`,
`E_OBJECT_STORE_UNREACHABLE`, `E_LEASE_LOST`). It is assembled from a closed
field set rather than from a formatted struct, so it can be republished into a
Pod event without leaking anything.

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
   on migration generation 1, in state `formatted` or
   `allocating`, with no format UUID recorded by the control-plane. Anywhere
   else an empty replica means the replica was lost, and formatting there would
   replace a filesystem with an empty one.
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
8. Start replication, mount FUSE in this process, and write `<state-dir>/ready`
   once the mount is in the process's mount table and the root inode answers.

## The writer lease

`lease_expires_at` is converted to this process's monotonic clock once, at
receipt, and never recomputed from the wall clock afterwards — a writer that
was frozen and thawed would otherwise resume believing it still holds the
lease. New writes stop at `expiry - write_stop_margin`. A wall-clock step
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
If the bound is exhausted the worker exits 69 — reported data loss — and still
releases the lease, since holding it costs the Agent a full TTL and the data is
lost either way.

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
| `meta.db` | restore or format | the SQLite metadata — this is the filesystem |
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
| `--metrics` | empty | one Prometheus listener per mount does not fit a node running many; `health.json` is the surface |
| Litestream compaction | L1 10 m, L2 1 h, L3 6 h | with snapshot 24 h and L0 retention 30 m, an idle mount costs 0.018 object ops/s |

These four are not configurable, and should not be. Everything that is tunable
is in the mount-options vocabulary above, so a measurement can change a value
without a worker rollout.

## Why Litestream runs as a child process

Litestream v0.5.17 opens the database it replicates with `modernc.org/sqlite`,
while JuiceFS opens the same file with `mattn/go-sqlite3`. Linking both into
one binary would put two independent SQLite library instances on one database
file inside one process, which SQLite does not support: POSIX advisory locks
are held per process, so closing any descriptor on the file drops every lock
the process holds, and each instance keeps its own inode and WAL-index registry
and cannot see the other's state. Two SQLite builds in two processes is what
the locking protocol is designed for.

Nothing is lost. `DB.SyncAndWait` is reachable as `litestream sync -wait` over
the control socket, a single SIGTERM makes `replicate` run its own shutdown
sync, and restore takes a TXID or a timestamp on the command line. Separate
processes also keep the crash domains apart.

The cost is about 36 MiB per mount. The way to recover it without the hazard is
one node-level Litestream watching many databases, which v0.5.17 supports
natively; whether a per-volume replica prefix can be expressed under a single
directory-watch config is the open question there.

## Not implemented

* ~~**Restore-time missing-block repair.**~~ Implemented by PLO-320 in
  `pkg/plori/restore`; see [`plori_restore.md`](plori_restore.md). The
  supervisor calls `Volume.RepairAfterRestore` after an unclean generation,
  between the session purge and the start of replication.
* **Litestream retention policy.** Snapshot interval, L0 retention and the
  compaction levels are set, but nothing prunes an abandoned epoch's metadata
  prefix after its volume is retired. Owner: PLO-320.
* **A metrics endpoint.** `health.json` is written for the plugin to read;
  the worker exposes no Prometheus surface of its own, and the JuiceFS metrics
  listener is disabled because one listener per mount does not fit a node
  running many. Owner: PLO-325.
* ~~**Recovery to `T_before` across nodes.**~~ Implemented by PLO-391. The
  MountSpec carries `durable_point` (the anchor, with the fencing epoch that
  produced it) and `restore_from_prefix` (that epoch's metadata prefix), so a
  worker starting on a node with no local `durable-point.json` restores the last
  proven durable point rather than the latest transaction. Both are omitted when
  the control-plane has no durable point on record, and the `PriorMetaPrefix`
  listing stays as the fallback for that case. One gap remains: the anchor is a
  wall-clock instant, so `replica_txid` is carried but not yet used — restoring
  BY TXID needs a `Replicator.Restore` that takes one.
* **A live quota hook.** A grant change is applied by rewriting the Format's
  capacity and inode ceiling, which the running client picks up on its next
  reload rather than immediately. Owner: PLO-324.
* **Restart supervision.** JuiceFS's own child supervisor is deliberately not
  in the picture, so a FUSE session that ends is reported as a non-zero exit
  and the plugin decides what happens next. Owner: PLO-366.

## Tests

`pkg/plori/mount` unit-tests the whole state machine against fakes: spec
refusals and their exit codes, the monotonic deadline arithmetic, the
three-way identity match, the fence marker's 412, the format gating, the
ordered shutdown, and the rendered Litestream config. The fence-marker test
drives a real AWS SDK client against an in-process shim that honours
`If-None-Match: *`.

`hack/plori-mount-e2e/run.sh` is the end-to-end proof and runs in the fork's
`plori` workflow, which has fuse3 and a pinned MinIO. It formats a volume,
mounts it, writes a file, stops with SIGTERM and requires exit 0, then restores
the replica into a fresh state directory under a new writer epoch and reads the
same bytes back.
