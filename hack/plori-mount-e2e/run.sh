#!/usr/bin/env bash
#
# End-to-end proof of the `juicefs plori-mount` lifecycle against a real FUSE
# mount, a real Litestream and a real S3 endpoint.
#
# It walks the whole contract once: format an empty volume, mount it, write a
# file, stop it with SIGTERM and require exit 0, then restore the metadata
# replica into a FRESH state directory under a new writer epoch and read the
# same bytes back. The second half is the one that matters — it is the
# difference between "the mount worked" and "the mount is recoverable", which
# is the property the whole per-Agent design rests on.
#
# Requires: fuse3, python3, a running S3 endpoint, and the binaries named below.
#
#   PLORI_BIN         path to juicefs.plori           (default ./juicefs.plori)
#   LITESTREAM_BIN    path to litestream v0.5.17      (default litestream)
#   S3_ENDPOINT       e.g. http://127.0.0.1:9000
#   S3_BUCKET         bucket that already exists
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
set -euo pipefail

PLORI_BIN=${PLORI_BIN:-./juicefs.plori}
LITESTREAM_BIN=${LITESTREAM_BIN:-litestream}
S3_ENDPOINT=${S3_ENDPOINT:?S3_ENDPOINT is required}
S3_BUCKET=${S3_BUCKET:?S3_BUCKET is required}
CP_PORT=${CP_PORT:-19871}

PLORI_BIN=$(readlink -f "$PLORI_BIN")
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)
VOLUME_ID=${VOLUME_ID:-e2e-$(date +%s)}
JOURNAL="$WORK/journal"
TOKEN="$WORK/token"
CP_PID=""
WORKER_PID=""

cleanup() {
  [ -n "$WORKER_PID" ] && kill -TERM "$WORKER_PID" 2>/dev/null || true
  [ -n "$CP_PID" ] && kill "$CP_PID" 2>/dev/null || true
  for mp in "$WORK"/mnt "$WORK"/mnt2; do
    mountpoint --quiet "$mp" 2>/dev/null && fusermount3 -u "$mp" 2>/dev/null || true
  done
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$WORK/mnt" "$WORK/mnt2" "$WORK/state" "$WORK/state2" "$WORK/cache" "$WORK/cache2"
echo "e2e-projected-token" > "$TOKEN"
: > "$JOURNAL"

python3 "$HERE/fake_control_plane.py" "$CP_PORT" "$TOKEN" "$JOURNAL" &
CP_PID=$!
for _ in $(seq 1 50); do
  curl -sf -o /dev/null "http://127.0.0.1:$CP_PORT/" -X POST -d '{}' \
    -H "Authorization: Bearer $(cat "$TOKEN")" && break
  sleep 0.1
done

write_spec() {
  local epoch=$1 out=$2 format_uuid=$3
  # A lease that is long relative to the test, with a renew interval short
  # enough that the loop actually runs several times before SIGTERM.
  # The field set below is the control-plane's, not a convenient subset of it:
  # services/control-plane/internal/storagespec emits every key here, and its
  # golden fixture (internal/storagespec/testdata/*.golden.json) is the
  # authority this file tracks. A spec that omits `format` or `may_format` is
  # refused with exit 64 (PLO-395), which is exactly what this harness must not
  # be able to hide.
  python3 - "$epoch" "$out" "$format_uuid" <<PY
import datetime, json, sys
epoch, out, format_uuid = int(sys.argv[1]), sys.argv[2], sys.argv[3]
expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=120)
endpoint, bucket = "$S3_ENDPOINT".rstrip("/"), "$S3_BUCKET".strip("/")
fmt = {
  "volume_id": "$VOLUME_ID",
  "bucket": endpoint + "/" + bucket,
  "data_prefix": "agents/$VOLUME_ID/",
  "meta_prefix": "agents-meta/$VOLUME_ID/",
  "trash_days": 1,
  "capacity_bytes": 1 << 30,
  "inodes": 100000,
  "grant_epoch": 1,
}
if format_uuid:
  fmt["expected_uuid"] = format_uuid
spec = {
  "storage_volume_id": "$VOLUME_ID",
  "format_uuid": format_uuid,
  "generation": 1,
  # An unformatted volume is `allocating`, and that lease IS the formatting
  # lease (PLO-373); once the control-plane has a Format.UUID it is `active`.
  "volume_state": "allocating" if not format_uuid else "active",
  "fence_epoch": epoch,
  "lease_expires_at": expires.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
  "lease_renew_interval": "2s",
  "write_stop_margin": "20s",
  "data_prefix": "agents/$VOLUME_ID/",
  "meta_prefix": "agents-meta/$VOLUME_ID/g%d/" % epoch,
  "fence_marker_key": "agents-meta/$VOLUME_ID/g%d/fence" % epoch,
  "grant": {"bytes": 1 << 30, "inodes": 100000, "epoch": 1, "acked_epoch": 0},
  "object_store": {
    "endpoint": endpoint,
    "bucket": bucket,
    "region": "${AWS_REGION:-us-east-1}",
    "credential_source": "node_secret",
  },
  "format": fmt,
  "may_format": not format_uuid,
  "mount_options": ["writeback", "heartbeat=30", "barrier_interval=5"],
  "issued_at": expires.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
}
open(out, "w").write(json.dumps(spec))
PY
}

run_worker() {
  local spec=$1 mnt=$2 state=$3 cache=$4 logfile=$5
  "$PLORI_BIN" plori-mount \
    --spec-file "$spec" \
    --mount-point "$mnt" \
    --state-dir "$state" \
    --cache-dir "$cache" \
    --control-plane-url "http://127.0.0.1:$CP_PORT" \
    --token-file "$TOKEN" \
    --litestream-bin "$LITESTREAM_BIN" \
    > "$logfile" 2>&1 &
  WORKER_PID=$!
  # The plugin starts each worker with Setpgid so it can signal the group; this
  # harness has to reproduce that or the group signal above has nothing to aim
  # at. `set -m` gives every background job its own group.
}

# Job control, so each background worker becomes its own process-group leader —
# the shape the plugin creates with Setpgid, and the shape the stop assertions
# above depend on (PLO-421).
set -m

await_ready() {
  local state=$1
  for _ in $(seq 1 300); do
    [ -f "$state/ready" ] && return 0
    kill -0 "$WORKER_PID" 2>/dev/null || return 1
    sleep 0.5
  done
  return 1
}

echo "== generation 1: format, mount, write, stop =="
write_spec 1 "$WORK/spec1.json" ""
run_worker "$WORK/spec1.json" "$WORK/mnt" "$WORK/state" "$WORK/cache" "$WORK/worker1.log"
await_ready "$WORK/state" || { cat "$WORK/worker1.log"; fail "worker never reported ready"; }

python3 -c "import json;d=json.load(open('$WORK/state/ready'));assert d['epoch']==1,d" \
  || fail "ready file does not name epoch 1"

# PLO-366: the FUSE loop runs IN THIS PROCESS. Upstream `juicefs mount` always
# re-execs itself (cmd/mount_unix.go launchMount), which is where the second
# juicefs process in the M0 footprint measurement came from; plori-mount calls
# serveMount directly. The proof is not "one process is running" — it is that
# the process holding the /dev/fuse connection IS the supervisor, because that
# is what makes a SIGKILL of the supervisor a dead mount rather than a
# restartable child.
fuse_fds=$(ls -l "/proc/$WORKER_PID/fd" 2>/dev/null | grep -c "/dev/fuse" || true)
[ "${fuse_fds:-0}" -ge 1 ] || fail "the supervisor pid $WORKER_PID holds no /dev/fuse descriptor: the FUSE loop is in another process"
# The only child a worker may have on this path is its litestream. A second
# juicefs would be the re-exec this design removed.
for child in $(pgrep -P "$WORKER_PID" 2>/dev/null || true); do
  child_comm=$(cat "/proc/$child/comm" 2>/dev/null || echo "")
  case "$child_comm" in
    litestream) ;;
    "") ;;
    *) fail "the worker forked a $child_comm child (pid $child); the FUSE loop must not be re-execed" ;;
  esac
done

echo "durable-bytes-from-generation-1" > "$WORK/mnt/probe"
mkdir -p "$WORK/mnt/dir/nested"
head -c 1048576 /dev/urandom > "$WORK/mnt/dir/nested/blob"
sha_before=$(sha256sum < "$WORK/mnt/dir/nested/blob" | cut -d' ' -f1)

# Let at least one renew tick and one periodic barrier happen, so health.json
# and the durable point are written by the run loop rather than only by stop.
sleep 8
[ -f "$WORK/state/health.json" ] || fail "health.json was never written"
python3 -c "
import json
h = json.load(open('$WORK/state/health.json'))
assert h['last_renew_ok'] is True, h
assert h['epoch'] == 1, h
assert h['grant_epoch_applied'] == 1, h
" || fail "health.json does not show a healthy renew loop"

# PLO-421: the plugin does not signal the worker, it signals the worker's
# process GROUP — kill(-pid, SIGTERM) — because that is how it reaches anything
# the worker forked. Signalling only the pid here would test a stop the
# production path never takes, and it is exactly what hid the bug: the
# litestream child was in that group, died a millisecond into the stop, and the
# final `sync -wait` 26 ms later found no control socket. Exit 69, no `clean`,
# lease left open, on EVERY ordered stop in a cluster.
kill -TERM -- "-$WORKER_PID" 2>/dev/null || kill -TERM "$WORKER_PID"
set +e
wait "$WORKER_PID"; worker_exit=$?
set -e
WORKER_PID=""
[ "$worker_exit" -eq 0 ] || { cat "$WORK/worker1.log"; fail "clean stop exited $worker_exit, want 0 (a group-wide SIGTERM must still leave the replicator alive long enough to finish the final sync)"; }

grep -q '/v1/internal/storage/lease/release' "$JOURNAL" || fail "the ordered stop never released the lease"
grep -q '/v1/internal/storage/durable-point' "$JOURNAL" || fail "no durable point was ever reported"
grep -q '/v1/internal/storage/usage' "$JOURNAL" || fail "usage was never reported"
grep -q 'UNAUTHORIZED' "$JOURNAL" && fail "the worker presented a token the control-plane rejected"
[ -f "$WORK/state/clean" ] || fail "the clean-stop marker was not written"

format_uuid=$(python3 -c "import json;print(json.load(open('$WORK/state/durable-point.json'))['volume'])" >/dev/null 2>&1; \
  "$PLORI_BIN" status "sqlite3://$WORK/state/meta.db" | python3 -c "import json,sys;print(json.load(sys.stdin)['Setting']['UUID'])")
[ -n "$format_uuid" ] || fail "could not read the format UUID back"

echo "== remount on the same node after a clean stop (PLO-422) =="
# The state directory is a hostPath: on the second mount of an Agent on a node
# it has already run on, `meta.db` is still there. It used to make the restore
# refuse and the worker exit 67, the kubelet retry, and a writer epoch burn per
# retry — an Agent could be mounted exactly once per node. A clean
# predecessor's database is the newest copy there is and must be adopted.
write_spec 4 "$WORK/spec4.json" "$format_uuid"
run_worker "$WORK/spec4.json" "$WORK/mnt" "$WORK/state" "$WORK/cache" "$WORK/worker4.log"
await_ready "$WORK/state" || { cat "$WORK/worker4.log"; fail "the second mount on this state dir never reported ready"; }
grep -q '"verdict":"adopted"' "$WORK/worker4.log" \
  || { cat "$WORK/worker4.log"; fail "a cleanly stopped local database was not adopted"; }
[ "$(cat "$WORK/mnt/probe")" = "durable-bytes-from-generation-1" ] \
  || fail "the adopted database does not hold what generation 1 wrote"
echo "written-by-generation-4" > "$WORK/mnt/probe4"
kill -TERM -- "-$WORKER_PID" 2>/dev/null || kill -TERM "$WORKER_PID"
set +e
wait "$WORKER_PID"; worker_exit=$?
set -e
WORKER_PID=""
[ "$worker_exit" -eq 0 ] || { cat "$WORK/worker4.log"; fail "the adopting worker exited $worker_exit, want 0"; }
[ -f "$WORK/state/clean" ] || fail "the adopting worker did not write a clean marker"

echo "== remount on the same node after a SIGKILL (PLO-422) =="
# The other half: an unclean predecessor's database is only durable to its last
# barrier, so it is set aside and the replica is restored over it. What must not
# happen either way is a refusal.
write_spec 5 "$WORK/spec5.json" "$format_uuid"
run_worker "$WORK/spec5.json" "$WORK/mnt" "$WORK/state" "$WORK/cache" "$WORK/worker5.log"
await_ready "$WORK/state" || { cat "$WORK/worker5.log"; fail "the third mount never reported ready"; }
killed5="$WORKER_PID"
kill -9 "$killed5"
set +e
wait "$killed5" 2>/dev/null
set -e
WORKER_PID=""
fusermount3 -uz "$WORK/mnt" 2>/dev/null || true
[ -f "$WORK/state/clean" ] && fail "a SIGKILLed worker left a clean marker"

write_spec 6 "$WORK/spec6.json" "$format_uuid"
run_worker "$WORK/spec6.json" "$WORK/mnt" "$WORK/state" "$WORK/cache" "$WORK/worker6.log"
await_ready "$WORK/state" || { cat "$WORK/worker6.log"; fail "the mount after a SIGKILL never reported ready (exit 67 is the PLO-422 regression)"; }
grep -q '"verdict":"set_aside"' "$WORK/worker6.log" \
  || { cat "$WORK/worker6.log"; fail "an unclean local database was not set aside"; }
[ -f "$WORK/state/meta.db.superseded" ] || fail "the superseded database was deleted rather than kept"
[ "$(cat "$WORK/mnt/probe")" = "durable-bytes-from-generation-1" ] \
  || fail "the restored database does not hold what generation 1 wrote"
kill -TERM -- "-$WORKER_PID" 2>/dev/null || kill -TERM "$WORKER_PID"
set +e
wait "$WORKER_PID"; worker_exit=$?
set -e
WORKER_PID=""
[ "$worker_exit" -eq 0 ] || { cat "$WORK/worker6.log"; fail "the restoring worker exited $worker_exit, want 0"; }

echo "== generation 2: restore into a fresh state dir under a new epoch =="
write_spec 2 "$WORK/spec2.json" "$format_uuid"
run_worker "$WORK/spec2.json" "$WORK/mnt2" "$WORK/state2" "$WORK/cache2" "$WORK/worker2.log"
await_ready "$WORK/state2" || { cat "$WORK/worker2.log"; fail "restored worker never reported ready"; }

[ -f "$WORK/state2/meta.db" ] || fail "the metadata replica was not restored"
[ "$(cat "$WORK/mnt2/probe")" = "durable-bytes-from-generation-1" ] \
  || fail "the file written by generation 1 did not survive the replica round trip"
sha_after=$(sha256sum < "$WORK/mnt2/dir/nested/blob" | cut -d' ' -f1)
[ "$sha_before" = "$sha_after" ] || fail "restored blob differs: $sha_before != $sha_after"

kill -TERM -- "-$WORKER_PID" 2>/dev/null || kill -TERM "$WORKER_PID"
set +e
wait "$WORKER_PID"; worker_exit=$?
set -e
WORKER_PID=""
[ "$worker_exit" -eq 0 ] || { cat "$WORK/worker2.log"; fail "restored worker exited $worker_exit, want 0"; }

echo "== refusals =="
# An unknown credential_source must be refused with exit 64, not fallen back on.
python3 - "$WORK/spec1.json" "$WORK/spec-bad.json" <<'PY'
import json, sys
spec = json.load(open(sys.argv[1]))
spec["object_store"]["credential_source"] = "vault_reference"
json.dump(spec, open(sys.argv[2], "w"))
PY
set +e
"$PLORI_BIN" plori-mount --spec-file "$WORK/spec-bad.json" --mount-point "$WORK/mnt" \
  --state-dir "$WORK/state-bad" --cache-dir "$WORK/cache-bad" \
  --control-plane-url "http://127.0.0.1:$CP_PORT" --token-file "$TOKEN" \
  --litestream-bin "$LITESTREAM_BIN" > "$WORK/bad.log" 2>&1
bad_exit=$?
set -e
[ "$bad_exit" -eq 64 ] || { cat "$WORK/bad.log"; fail "unknown credential_source exited $bad_exit, want 64"; }
# The plugin republishes the last stderr line into a kubelet event, so it must
# be valid JSON with a typed code and no credential in it (threat-model F-11).
tail -1 "$WORK/bad.log" | python3 -c "
import json, sys
line = json.loads(sys.stdin.read())
assert line['error'] == 'E_SPEC_INVALID', line
assert line['exit'] == 64, line
for k, v in line.items():
    assert 'secret' not in k.lower(), line
    assert '${AWS_SECRET_ACCESS_KEY}' not in str(v), 'terminal line leaked the object key'
" || fail "the terminal stderr line is not a safe typed JSON object"

echo "== restart supervision: a SIGKILLed worker leaves a DEAD mount, never a half-attached one =="
# PLO-366. JuiceFS's own supervisor used to restart a killed FUSE child. There
# is no child any more, so the guarantee has to come from the other side: the
# mount point must become UNMISTAKABLY dead, so the plugin's liveness check
# (`ready` gone / ENOTCONN / the -pgid probe) fires the abnormal-exit path
# instead of leaving an Agent writing into a filesystem nobody is serving.
write_spec 3 "$WORK/spec3.json" "$format_uuid"
run_worker "$WORK/spec3.json" "$WORK/mnt" "$WORK/state" "$WORK/cache" "$WORK/worker3.log"
await_ready "$WORK/state" || { cat "$WORK/worker3.log"; fail "worker never reported ready before the kill" ; }
cat "$WORK/mnt/probe" > /dev/null || fail "the mount does not serve before the kill"
killed_pid="$WORKER_PID"
kill -9 "$killed_pid"
set +e
wait "$killed_pid" 2>/dev/null
set -e
WORKER_PID=""
for _ in $(seq 1 100); do
  kill -0 "$killed_pid" 2>/dev/null || break
  sleep 0.1
done
kill -0 "$killed_pid" 2>/dev/null && fail "the worker survived SIGKILL"
# Nothing restarted it, and nothing may: one process per mount means the mount
# is gone with it.
pgrep -P "$killed_pid" >/dev/null 2>&1 && fail "a child outlived the SIGKILLed worker"
# The mount point is detectably dead rather than silently empty. ENOTCONN is
# what the kernel answers once the FUSE server is gone, and it is what the
# plugin keys on; a mount that answered normally here would be the "half
# attached" state this issue exists to rule out.
set +e
probe_err=$(cat "$WORK/mnt/probe" 2>&1)
probe_rc=$?
set -e
[ "$probe_rc" -ne 0 ] || fail "the mount point still served reads after its only process was killed"
case "$probe_err" in
  *"not connected"*|*"Transport endpoint"*|*"Socket not connected"*) ;;
  *) fail "reading a dead mount gave $probe_err, want a transport-endpoint error the plugin can classify" ;;
esac
# And it is still a mountpoint, so the plugin's unpublish has something to
# detach: a kill that also unmounted would hide the failure from the guard.
mountpoint --quiet "$WORK/mnt" || fail "the kernel dropped the mount entry; the plugin would see success"
fusermount3 -u "$WORK/mnt" 2>/dev/null || fusermount3 -uz "$WORK/mnt" 2>/dev/null || true

echo "plori-mount end-to-end lifecycle verified"
