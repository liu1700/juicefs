#!/usr/bin/env python3
"""PLO-1172 real-process faults; invoked by run.sh after mount readiness."""

from datetime import datetime
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time


def events(path):
    result = []
    for line in path.read_text().splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue  # JuiceFS also writes unstructured logs and partial lines.
        if isinstance(item, dict) and "event" in item:
            result.append(item)
    return result


def alive(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def children(pid):
    # Go may start the child from any OS thread, not only the main thread.
    found = set()
    for task in Path(f"/proc/{pid}/task").glob("*/children"):
        try:
            candidates = task.read_text().split()
        except FileNotFoundError:
            continue
        for child in candidates:
            try:
                args = Path(f"/proc/{child}/cmdline").read_bytes().split(b"\0")
                if b"replicate" in args and alive(int(child)):
                    found.add(int(child))
            except FileNotFoundError:
                pass
    return found


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def require_no_terminal(records):
    require(not any(e["event"] == "plori_mount_terminal" or
                    e.get("error") == "E_REPLICATION_FAILED" for e in records),
            "supervisor emitted a terminal/replication failure event")


def require_replication_terminal(records):
    terminal = [e for e in records if e["event"] == "plori_mount_terminal"]
    require(len(terminal) == 1 and terminal[0].get("error") == "E_REPLICATION_FAILED"
            and terminal[0].get("exit") == 69, f"wrong terminal event: {terminal}")


def wait_for(check, seconds, message):
    deadline = time.monotonic() + seconds
    while True:
        if check():
            return
        require(time.monotonic() < deadline, message)
        time.sleep(0.1)


def writer(mount, progress):
    # Record completion only after fsync so buffered writes cannot fake progress.
    with progress.open("w", buffering=1) as output:
        for i in range(100000):
            with (mount / f"fault-write-{i}").open("wb") as target:
                target.write(f"write {i}\n".encode())
                target.flush()
                os.fsync(target.fileno())
            output.write(f"{i}\n")
            time.sleep(0.1)


def timestamp(record):
    return datetime.fromisoformat(record["ts"].replace("Z", "+00:00")).timestamp()


def matching(records, name):
    return [e for e in records if e["event"] == name]


def first_running_failure(records):
    failures = [e for e in matching(records, "replication_probe_failed")
                if e.get("replicator") == "running"]
    if not failures:
        return None
    require(failures[0].get("probe_failures") == 1, "first running failure must count as 1")
    return failures[0]


def require_recovery(records, reason=None):
    require_no_terminal(records)
    require(not matching(records, "replication_restart_failed"), "restart attempt failed")
    recovered = matching(records, "replication_recovered")
    require(recovered, "missing replication_recovered")
    restarts = matching(records, "replication_restarted")
    if reason is None:
        require(not restarts, f"short stall restarted: {restarts}")
        require(first_running_failure(records), "short stall missed running probe failure")
        return
    require(len(restarts) == 1 and restarts[0].get("reason") == reason,
            f"expected one restart with reason={reason}, got {restarts}")
    restart = restarts[0]
    require(records.index(restart) < records.index(recovered[0]), "recovery preceded restart")
    failures = matching(records[:records.index(restart)], "replication_probe_failed")
    if reason == "probe_failures":
        first = first_running_failure(failures)
        require(first, "hung child missed running failure")
        counts = [e.get("probe_failures") for e in failures if e.get("replicator") == "running"]
        require(counts == [1, 2, 3], f"wrong running failure counts: {counts}")
        elapsed = timestamp(restart) - timestamp(first)
        require(0 <= elapsed <= 20, f"hung-child restart took {elapsed:.3f}s, limit 20s")
    else:
        require(any(e.get("replicator") == "gone" for e in failures), "crash missed gone probe")


def require_stop_deadlines(records, first, lease_stop):
    stops = matching(records, "replication_failed_stop")
    require(len(stops) == 1, f"expected one failed stop, got {stops}")
    require_replication_terminal(records)
    stop = timestamp(stops[0])
    terminal = timestamp(matching(records, "plori_mount_terminal")[0])
    deadline = min(timestamp(first) + 30, lease_stop)
    require(deadline - 0.1 <= stop <= deadline + 5, "failed stop outside recovery window + 5s")
    require(stop <= terminal <= timestamp(first) + 65, "terminal outside recovery window + 35s")
    require(terminal <= lease_stop, "terminal after lease stop instant")


def run(pid, mount, state, log):
    progress = state / "writer-progress"
    stopped = set()
    with (state / "writer.log").open("w") as output:
        writing = subprocess.Popen(
            [sys.executable, __file__, "--writer", str(mount), str(progress)],
            stdout=output, stderr=subprocess.STDOUT,
        )
        last_count, last_progress = 0, time.monotonic()

        def healthy():
            nonlocal last_count, last_progress
            require(alive(pid), "supervisor exited during fault")
            require(os.path.ismount(mount), "FUSE mount disappeared")
            require_no_terminal(events(log))
            require(writing.poll() is None, "writer failed; see state/writer.log")
            count = len(progress.read_text().splitlines()) if progress.exists() else 0
            if count > last_count:
                last_count, last_progress = count, time.monotonic()
            require(time.monotonic() - last_progress < 5, "writer made no fsync progress for 5 s")
            return count

        def child():
            found = children(pid)
            require(len(found) == 1, f"expected one litestream replicate child, got {found}")
            return found.pop()

        def stable(duration, expected):
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                healthy()
                require(child() == expected, "litestream PID changed during live-child pause")
                time.sleep(0.1)

        def recovered(offset):
            healthy()
            return bool(matching(events(log)[offset:], "replication_recovered"))

        try:
            wait_for(lambda: healthy() >= 3, 10, "writer did not start")
            original, offset, before = child(), len(events(log)), healthy()
            stopped.add(original)
            os.kill(original, signal.SIGSTOP)

            def failed():
                healthy()
                return first_running_failure(events(log)[offset:])

            wait_for(failed, 20, "no first running probe failure")
            os.kill(original, signal.SIGCONT)
            stopped.discard(original)
            wait_for(lambda: recovered(offset), 15, "no recovery after short stall")
            stable(6, original)
            require_recovery(events(log)[offset:])
            require(healthy() > before, "writer did not progress during short stall")
            print(f"PASS short stall: PID {original} unchanged, no restart, recovered, writer progressing", flush=True)

            sock = state / "litestream.sock"
            for reason, fault in (("probe_failures", signal.SIGSTOP), ("gone", signal.SIGKILL)):
                original, offset, before = child(), len(events(log)), healthy()
                old_socket = sock.stat()
                if fault == signal.SIGSTOP:
                    stopped.add(original)
                os.kill(original, fault)
                seen = set()
                if reason == "probe_failures":
                    wait_for(failed, 20, "hung child produced no running probe failure")
                restart_deadline = time.monotonic() + 20

                def restarted():
                    healthy()
                    records = events(log)[offset:]
                    if not matching(records, "replication_restarted"):
                        require(time.monotonic() < restart_deadline, "no restart within 20s of first failure")
                    seen.update(children(pid) - {original})
                    require(len(seen) <= 1, f"more than one replacement child: {seen}")
                    try:
                        current = sock.stat()
                    except FileNotFoundError:
                        return False
                    return (len(seen) == 1 and children(pid) == seen and
                            stat.S_ISSOCK(current.st_mode) and
                            (current.st_ino, current.st_ctime_ns) !=
                            (old_socket.st_ino, old_socket.st_ctime_ns) and recovered(offset))

                wait_for(restarted, 35, "replacement child/socket/recovery did not appear")
                replacement = next(iter(seen))
                stopped.discard(original)
                stable(12, replacement)
                require_recovery(events(log)[offset:], reason)
                require(healthy() > before, "writer did not progress across replacement")
                print(f"PASS {reason}: {original} -> {replacement}, one restart, new socket, recovered, writer progressing", flush=True)

            if os.environ.get("PLORI_E2E_LONG_PAUSE", "0") == "1":
                offset = len(events(log))
                health = json.loads((state / "health.json").read_text())
                # This already-granted deadline is conservative: later renewals
                # can only extend it in this fake control plane.
                lease_stop = timestamp({"ts": health["lease_expires_at"]}) - 20 - health["projected_drain_seconds"]
                start, first = time.monotonic(), None
                while not matching(events(log)[offset:], "replication_failed_stop"):
                    for replacement in children(pid) - stopped:
                        try:
                            os.kill(replacement, signal.SIGSTOP)
                            stopped.add(replacement)
                        except ProcessLookupError:
                            pass
                    records = events(log)[offset:]
                    first = first or first_running_failure(records)
                    require_no_terminal(records)
                    require(alive(pid), "supervisor exited without replication_failed_stop")
                    deadline = min(timestamp(first) + 35, lease_stop) if first else time.time() + 1
                    require(time.time() <= deadline and time.monotonic() - start < 55,
                            "no failed stop within recovery window + 5s")
                    time.sleep(0.01)
                require(first, "long stall produced no running failure")
                require(time.monotonic() - start > 30, "long fault lasted less than 30s")
                require(not matching(events(log)[offset:], "replication_recovered"),
                        "a replacement recovered during continuous fault injection")
                # The recovery decision is final; let ordered shutdown finish its sync.
                for replacement in stopped:
                    if alive(replacement):
                        os.kill(replacement, signal.SIGCONT)
                stopped.clear()
                wait_for(lambda: not alive(pid), 35, "supervisor did not exit after failed stop")
                require_stop_deadlines(events(log)[offset:], first, lease_stop)
                require(time.time() <= min(timestamp(first) + 65, lease_stop), "process exit missed deadline")
                print("PASS continuous SIGSTOP: failed stop within window, E_REPLICATION_FAILED/69 before lease stop", flush=True)
        finally:
            for replacement in stopped:
                if alive(replacement):
                    os.kill(replacement, signal.SIGCONT)
            writing.terminate()
            try:
                writing.wait(timeout=3)
            except subprocess.TimeoutExpired:
                writing.kill()
                writing.wait(timeout=3)


if __name__ == "__main__":
    if sys.argv[1] == "--writer":
        writer(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        run(int(sys.argv[1]), *(Path(p) for p in sys.argv[2:]))
