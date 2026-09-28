The default `run.sh` invocation exercises the mount lifecycle. Run PLO-1172
replication fault injection with the same binary, S3 and AWS environment:

```sh
PLORI_E2E_SCENARIO=replication-faults hack/plori-mount-e2e/run.sh
```

This formats a fresh volume, starts a real FUSE mount and fake control plane,
and writes and fsyncs small files every 100 ms. PR CI runs these three cases:

- Short stall: SIGSTOP the Litestream child and SIGCONT immediately after the
  first `replication_probe_failed` with `replicator=running`, `probe_failures=1`.
  Require `replication_recovered`, the same PID and no restart.
- Hung child: leave the child SIGSTOPped. Require running failure counts 1, 2, 3,
  exactly one `replication_restarted` with `reason=probe_failures` within 20 s
  of the first failure, a new PID and Unix socket, then `replication_recovered`.
  The supervisor's 5 s SIGTERM grace cannot wake a stopped child; SIGKILL must
  reap it before replacement. The harness does not send that kill.
- Crash: SIGKILL once. Require a `replicator=gone` failure, exactly one restart
  with `reason=gone`, a new PID and socket, then `replication_recovered`.

All three require the mount to stay up, no terminal event, and continued writer
fsync progress with no gap of 5 s. Observe the recovered short stall for 6 s and
both replacements for 12 s to detect unintended restarts. Restart failures fail
the scenario. Recovery must use the exact event name `replication_recovered`.
The old `PLORI_E2E_PAUSES` and `PLORI_E2E_RECOVERY_EVENT` overrides are removed.

`PLORI_E2E_LONG_PAUSE=1` adds an optional fourth case (off in PR CI): SIGSTOP the
child and every replacement as it appears, polling every 10 ms, until
`replication_failed_stop`. Require continuous failure for longer than 30 s,
with the stop decision at the 30 s recovery deadline plus at most 5 s scheduling
margin. Then resume surviving children so ordered shutdown can finish its sync.
Require `E_REPLICATION_FAILED` / exit 69 and actual process exit within the
30 s window plus 35 s shutdown margin, and before the lease stop instant.
The lease bound uses the health snapshot at injection (expiry minus the spec's
20 s write-stop margin and projected drain); subsequent fake-control-plane
renewals only extend this conservative bound. The optional case does not test
lease loss itself or require successful writes during expected shutdown.

Logs and writer progress remain in the printed temporary artifact directory.
CI uses the lifecycle step's checksum-verified Litestream v0.5.17 binary and
running pinned MinIO. The default fault cases run on PRs to main.

Checks that do not require external services:

```sh
bash -n hack/plori-mount-e2e/run.sh
shellcheck hack/plori-mount-e2e/run.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s hack/plori-mount-e2e -p '*_test.py' -v
```
