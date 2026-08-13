---
title: Plori immutable-chunk profile
---

This profile is only for the Plori/Orlop data path. Orlop stores immutable,
content-addressed FastCDC chunks as individual files, writes them once with
`temp + fsync + rename`, reads each chunk in full, and eventually unlinks it.
Generic file system tuning advice does not necessarily apply to this workload.

## Selected profile

The selected production profile retains the 4 MiB block size. Format new
volumes explicitly so the choice is visible and does not drift:

```shell
juicefs format \
  --storage s3 \
  --bucket "$S3_BUCKET" \
  --block-size 4M \
  "$META_URL" plori-orlop-v2
```

Use these initial mount settings and expose metrics on a private listener:

```shell
juicefs mount \
  --writeback \
  --max-uploads 20 \
  --buffer-size 300M \
  --cache-size 10G \
  --cache-large-write=false \
  --prefetch 0 \
  --max-readahead 16M \
  --metrics 127.0.0.1:9567 \
  "$META_URL" /mnt/juicefs
```

The explicit 20-upload limit remains the measured baseline instead of
assuming that more concurrency is faster. The selected 300 MiB buffer can hold
twenty 8 MiB uploads plus 140 MiB of working headroom, so this profile does not
increase per-mount memory without evidence.
Prefetch is disabled because Orlop reads whole chunk files; a 16 MiB readahead
cap covers the largest chunk without an unbounded read window. The persistent
10 GiB cache is retained for writeback staging and restart recovery. Do not
remove it merely to avoid duplication with Orlop's client cache:
`cache-size=0` also disables JuiceFS writeback.

Every graceful drain must run a remote durability barrier before unmount:

```shell
juicefs durability --timeout 10m /mnt/juicefs
juicefs umount --flush /mnt/juicefs
```

## Evidence and benchmark

`hack/plori-benchmark/corpus.json` contains 512 MiB across 131 chunks generated
by Orlop's FastCDC implementation at commit
`b11e03e804fcff376749d9ab96e193cbb3e66b6b`. Terminal chunks explain sizes
below the nominal 1 MiB minimum.

| JuiceFS block size | Estimated data objects | Change from 4 MiB |
| --- | ---: | ---: |
| 4 MiB | 202 | baseline |
| 8 MiB | 137 | -32.2% |
| 16 MiB | 131 | -35.1% |

Eight MiB removes 32.2% of the 4 MiB request amplification. Sixteen MiB saves
only another 4.4% and uses JuiceFS's maximum block size, so 8 MiB is the only
migration candidate. ARM64 Docker/MinIO functional validation matched all
three predicted PUT counts exactly and completed every durability barrier
with zero failed uploads. The final three-run, rotated-order median was:

| Block | PUTs | Write MiB/s | First read MiB/s | Write p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 4 MiB | 202 | 275.4 | 801.9 | 64.0 |
| 8 MiB | 137 | 179.8 | 633.7 | 89.7 |
| 16 MiB | 131 | 227.0 | 824.1 | 104.8 |

The shared local MinIO run was noisy and reads made zero S3 GETs while the
writeback cache remained populated, so these are not production or cold-read
figures. Nevertheless, both larger blocks crossed a predefined write or p99
rollback threshold; the automated recommendation correctly retained 4 MiB.
Existing Plori observations provide production rollback floors: durable
sequential writes were reported as 196.9 MB/s and first/warm reads as
16.3-16.7 MB/s. The release therefore keeps 4 MiB until a same-host production
canary proves that 8 MiB passes every gate.

Create three fresh disposable volumes and mounts, copy
`hack/plori-benchmark/targets.example.json`, then run:

```shell
make test.plori.benchmark
python3 hack/plori-benchmark/benchmark.py \
  --targets /secure/path/targets.json \
  --repetitions 5 \
  --work-dir /local-nvme/plori-bench \
  --report-json /secure/path/plori-benchmark.json \
  --report-md /secure/path/plori-benchmark.md
```

The timed path performs the Orlop write pattern, a durability barrier, first
and warm full reads, and deletion. It records p50/p95/p99 latency, throughput,
PUT/GET/DELETE counts and bytes, block-cache counters, upload concurrency, and
writeback depth/age. Set `node_metrics_url` when node-exporter is available to
include local disk counters alongside JuiceFS process CPU and memory. A target
may define argv arrays named `recovery_command` and `cold_prepare_command`;
`{mount}` is replaced with the target mount. They run without a shell. Use them
only on isolated benchmark hosts to restart a mount with populated staging
data or clear caches before the first read.

Before writing data, the tool reads the mount's protected `.config` and rejects
a target whose actual format block size differs from its declaration. Run it
as the mount owner (normally root) and retain the JSON environment/version
record with the benchmark evidence.

Use at least five repetitions for a production decision. Each repetition
rotates the target order; the summary and recommendation use medians while the
JSON retains every raw run.

## Canary and rollback gates

Create and promote an 8 MiB candidate volume only when all of the following
hold for both the direct JuiceFS benchmark and an Orlop end-to-end canary:

- estimated and observed PUT counts do not exceed the 8 MiB result;
- write and first-read throughput are at least 90% of the 4 MiB same-host
  baseline (and at least 177 MB/s write and 14.7 MB/s read against the
  historical production floor);
- write p99 and durability time are no more than 120% of the 4 MiB same-host
  baseline;
- object request errors and failed uploads remain zero;
- oldest pending upload age stays below 60 seconds, including during drain;
- restart recovery drains the populated staging directory within the pod
  termination budget;
- Orlop p95/p99 write, read, and GC latency do not regress by more than 10%.

Roll back mount tuning first if these gates fail. Increase `max-uploads` only
when upload utilization remains above 90% for ten minutes and the cache disk,
S3 latency, CPU, and memory panels show headroom; benchmark it together with
`buffer-size`. Never modify block size in place.

## New-volume migration and compatibility

Block size is stored in volume metadata and is format-immutable. Do not migrate
the current volume for this release. If an 8 MiB production canary later passes
all gates, use a new Redis namespace and S3 prefix/bucket, mount old and new
volumes with the same released Plori image, and bulk-copy the immutable Orlop
store. Quiesce writes, run the old volume's durability barrier, perform and
verify the final delta, then point one canary Orlop server at the new mount.
Keep the old volume read-only until the rollback window closes.

The candidate 8 MiB format is within JuiceFS's existing supported range, so it
does not require a metadata version or `MinClientVersion` change.
Operationally, Plori still requires one immutable client image digest for
format, migration, and mount. Do not mix an older client into a live
writeback/drain sequence.

Import `deploy/plori/grafana-dashboard.json` and
`deploy/plori/prometheus-rules.yaml`. The rules alert on failed uploads, stale
backlogs/barriers, S3 request errors, and sustained saturation of the selected
20-upload limit.
