#!/usr/bin/env python3
"""Benchmark the immutable Orlop chunk workload on disposable JuiceFS mounts."""

import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid


METRIC_PREFIXES = (
    "juicefs_blockcache_",
    "juicefs_go_",
    "juicefs_object_request_",
    "juicefs_process_",
    "juicefs_staging_",
    "juicefs_writeback_",
    "node_disk_",
)
GAUGE_NAMES = {
    "juicefs_blockcache_bytes",
    "juicefs_go_goroutines",
    "juicefs_go_memstats_alloc_bytes",
    "juicefs_go_memstats_sys_bytes",
    "juicefs_object_request_uploading",
    "juicefs_process_resident_memory_bytes",
    "juicefs_process_virtual_memory_bytes",
    "juicefs_staging_block_bytes",
    "juicefs_writeback_pending_blocks",
    "juicefs_writeback_pending_bytes",
    "juicefs_writeback_oldest_pending_age_seconds",
    "juicefs_writeback_failed_uploads",
}
TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$")
SAMPLE_RE = re.compile(r"^([^\s]+)\s+([^\s]+)(?:\s+\d+)?$")


def load_corpus(path):
    corpus = json.loads(path.read_text(encoding="utf-8"))
    if corpus.get("schema_version") != 1:
        raise ValueError("unsupported corpus schema")
    sizes = corpus.get("sizes_bytes")
    if not isinstance(sizes, list) or not sizes or any(not isinstance(n, int) or n <= 0 for n in sizes):
        raise ValueError("corpus sizes_bytes must be a non-empty list of positive integers")
    expected = corpus.get("provenance", {}).get("input_bytes")
    if expected is not None and sum(sizes) != expected:
        raise ValueError(f"corpus byte total {sum(sizes)} does not match provenance {expected}")
    return corpus


def load_targets(path, allow_unmounted=False):
    document = json.loads(path.read_text(encoding="utf-8"))
    targets = document.get("targets")
    if not isinstance(targets, list) or not targets:
        raise ValueError("targets must be a non-empty list")
    names = set()
    mounts = set()
    for target in targets:
        if not isinstance(target, dict):
            raise ValueError("each target must be an object")
        name = target.get("name", "")
        if not TARGET_NAME_RE.fullmatch(name) or name in names:
            raise ValueError(f"invalid or duplicate target name: {name!r}")
        names.add(name)
        if target.get("block_size_mib") not in (4, 8, 16):
            raise ValueError(f"{name}: block_size_mib must be 4, 8, or 16")
        mount = pathlib.Path(target.get("mount", "")).expanduser().resolve()
        if not mount.is_dir():
            raise ValueError(f"{name}: mount is not a directory: {mount}")
        if mount in mounts:
            raise ValueError(f"{name}: duplicate mountpoint: {mount}")
        mounts.add(mount)
        if not allow_unmounted and not os.path.ismount(mount):
            raise ValueError(f"{name}: target is not a mountpoint: {mount}")
        target["mount"] = str(mount)
        for key in ("metrics_url", "node_metrics_url"):
            url = target.get(key)
            if key == "node_metrics_url" and url is None:
                continue
            parsed = urllib.parse.urlparse(url if isinstance(url, str) else "")
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise ValueError(f"{name}: {key} must be an HTTP(S) URL")
        if not allow_unmounted:
            actual_block_size = read_mount_block_size_mib(mount)
            if actual_block_size != target["block_size_mib"]:
                raise ValueError(
                    f"{name}: mounted volume uses {actual_block_size} MiB blocks, "
                    f"not declared {target['block_size_mib']} MiB"
                )
            target["observed_block_size_mib"] = actual_block_size
        for key in ("recovery_command", "cold_prepare_command"):
            command = target.get(key)
            if command is not None and (
                not isinstance(command, list)
                or not command
                or any(not isinstance(part, str) or not part for part in command)
            ):
                raise ValueError(f"{name}: {key} must be a non-empty argv array")
    return targets


def read_mount_block_size_mib(mount):
    for name in (".jfs.config", ".config"):
        path = mount / name
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"cannot read mounted JuiceFS config {path}: {error}") from error
        try:
            block_size_kib = config["Format"]["BlockSize"]
        except (KeyError, TypeError) as error:
            raise ValueError(f"mounted JuiceFS config lacks Format.BlockSize: {path}") from error
        if (
            not isinstance(block_size_kib, int)
            or block_size_kib <= 0
            or block_size_kib % 1024
        ):
            raise ValueError(f"invalid Format.BlockSize in {path}: {block_size_kib!r}")
        return block_size_kib // 1024
    raise ValueError(f"cannot find .jfs.config or .config under mountpoint {mount}")


def generate_sources(root, sizes):
    root.mkdir(parents=True, exist_ok=True)
    sources = []
    for index, size in enumerate(sizes):
        rng = random.Random(0x6F726C6F70 + index)
        digest = hashlib.sha256()
        source = root / f"{index:04d}.chunk"
        remaining = size
        with source.open("wb") as stream:
            while remaining:
                block = rng.randbytes(min(remaining, 1024 * 1024))
                stream.write(block)
                digest.update(block)
                remaining -= len(block)
        sources.append((source, digest.hexdigest(), size))
    return sources


def parse_prometheus(text):
    samples = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = SAMPLE_RE.match(line)
        if not match:
            continue
        series, raw_value = match.groups()
        metric = series.split("{", 1)[0]
        if not metric.startswith(METRIC_PREFIXES):
            continue
        try:
            value = float(raw_value)
        except ValueError:
            continue
        if math.isfinite(value):
            samples[series] = value
    return samples


def scrape_metrics(url):
    if not url:
        return {}
    with urllib.request.urlopen(url, timeout=3) as response:
        return parse_prometheus(response.read().decode("utf-8"))


def metric_delta(before, after):
    result = {}
    for series, value in after.items():
        metric = series.split("{", 1)[0]
        if metric in GAUGE_NAMES:
            continue
        previous = before.get(series, 0.0)
        if value < previous:
            continue
        delta = value - previous
        if delta:
            result[series] = delta
    return result


def summarize_metrics(before, after, maxima):
    deltas = metric_delta(before, after)
    object_requests = {method: 0 for method in ("PUT", "GET", "DELETE")}
    object_bytes = {method: 0 for method in ("PUT", "GET", "DELETE")}
    for series, value in deltas.items():
        method_match = re.search(r'method="([A-Z]+)"', series)
        if not method_match:
            continue
        method = method_match.group(1)
        if "object_request_durations_histogram_seconds_count" in series:
            object_requests[method] = object_requests.get(method, 0) + int(value)
        elif "object_request_data_bytes" in series:
            object_bytes[method] = object_bytes.get(method, 0) + int(value)
    return {
        "object_requests": object_requests,
        "object_bytes": object_bytes,
        "counter_deltas": deltas,
        "max_gauges": maxima,
        "final_gauges": {
            series: value
            for series, value in after.items()
            if series.split("{", 1)[0] in GAUGE_NAMES
        },
    }


class MetricsSampler:
    def __init__(self, urls):
        if isinstance(urls, str):
            urls = [urls]
        self.urls = [url for url in (urls or []) if url]
        self.maxima = {}
        self.errors = []
        self.stop_event = threading.Event()
        self.thread = None

    def sample(self):
        values = {}
        for url in self.urls:
            try:
                values.update(scrape_metrics(url))
            except Exception as error:  # retain scrape failures in the evidence
                message = f"{url}: {error}"
                if message not in self.errors:
                    self.errors.append(message)
        for series, value in values.items():
            if series.split("{", 1)[0] in GAUGE_NAMES:
                self.maxima[series] = max(value, self.maxima.get(series, value))
        return values

    def start(self):
        if not self.urls or self.thread is not None:
            return
        self.stop_event.clear()

        def poll():
            while not self.stop_event.is_set():
                self.sample()
                self.stop_event.wait(0.25)

        self.thread = threading.Thread(target=poll, name="plori-metrics", daemon=True)
        self.thread.start()

    def stop(self):
        if self.thread is None:
            return
        self.stop_event.set()
        self.thread.join(timeout=5)
        self.thread = None


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def phase_summary(duration, bytes_processed, latencies):
    return {
        "duration_seconds": duration,
        "bytes": bytes_processed,
        "throughput_mib_s": bytes_processed / (1024 * 1024) / duration if duration else 0.0,
        "file_latency_ms": {
            "p50": percentile(latencies, 0.50) * 1000,
            "p95": percentile(latencies, 0.95) * 1000,
            "p99": percentile(latencies, 0.99) * 1000,
        },
    }


def timed_write(benchmark_root, sources):
    latencies = []
    started = time.monotonic()
    for index, (source, digest, _) in enumerate(sources):
        shard = benchmark_root / "objects" / digest[:2]
        shard.mkdir(parents=True, exist_ok=True)
        final = shard / digest
        temporary = shard / f".{digest}.{index}.tmp"
        item_started = time.monotonic()
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer, 1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(temporary, final)
        latencies.append(time.monotonic() - item_started)
    return phase_summary(time.monotonic() - started, sum(item[2] for item in sources), latencies)


def timed_read(benchmark_root, sources):
    latencies = []
    total = 0
    started = time.monotonic()
    for _, digest, expected_size in sources:
        item_started = time.monotonic()
        read_bytes = 0
        with (benchmark_root / "objects" / digest[:2] / digest).open("rb") as reader:
            while block := reader.read(1024 * 1024):
                read_bytes += len(block)
        if read_bytes != expected_size:
            raise IOError(f"short read for {digest}: {read_bytes} != {expected_size}")
        total += read_bytes
        latencies.append(time.monotonic() - item_started)
    return phase_summary(time.monotonic() - started, total, latencies)


def timed_delete(benchmark_root, sources):
    latencies = []
    started = time.monotonic()
    for _, digest, _ in sources:
        item_started = time.monotonic()
        (benchmark_root / "objects" / digest[:2] / digest).unlink()
        latencies.append(time.monotonic() - item_started)
    return phase_summary(time.monotonic() - started, 0, latencies)


def run_command(command, mount):
    argv = [part.replace("{mount}", str(mount)) for part in command]
    started = time.monotonic()
    completed = subprocess.run(argv, check=True, capture_output=True, text=True)
    return {
        "argv": argv,
        "duration_seconds": time.monotonic() - started,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def durability_barrier(juicefs, mount, timeout):
    command = [juicefs, "durability", "--timeout", timeout, "--json", str(mount)]
    result = run_command(command, mount)
    result["response"] = json.loads(result["stdout"])
    return result


def estimated_objects(sizes, block_size_mib):
    block_size = block_size_mib * 1024 * 1024
    return sum(math.ceil(size / block_size) for size in sizes)


def run_target(target, sources, sizes, juicefs, timeout, skip_durability=False):
    mount = pathlib.Path(target["mount"])
    benchmark_root = mount / f".plori-benchmark-{uuid.uuid4().hex}"
    sampler = MetricsSampler([target.get("metrics_url"), target.get("node_metrics_url")])
    baseline = sampler.sample()
    result = {
        "name": target["name"],
        "mount": str(mount),
        "block_size_mib": target["block_size_mib"],
        "estimated_data_objects": estimated_objects(sizes, target["block_size_mib"]),
        "phases": {},
    }
    whole_maxima = {}

    def finish_phase(before):
        sampler.stop()
        after = sampler.sample()
        phase_maxima = dict(sampler.maxima)
        for series, value in phase_maxima.items():
            whole_maxima[series] = max(value, whole_maxima.get(series, value))
        sampler.maxima.clear()
        return after, summarize_metrics(before, after, phase_maxima)

    try:
        benchmark_root.mkdir(mode=0o700)
        sampler.maxima.clear()
        before_write = sampler.sample()
        sampler.start()
        result["phases"]["write"] = timed_write(benchmark_root, sources)
        _, write_metrics = finish_phase(before_write)
        if target.get("recovery_command"):
            result["phases"]["recovery"] = run_command(target["recovery_command"], mount)
        if not skip_durability:
            result["phases"]["durability"] = durability_barrier(juicefs, mount, timeout)
        if target.get("cold_prepare_command"):
            result["phases"]["cold_prepare"] = run_command(target["cold_prepare_command"], mount)
        before_first_read = sampler.sample()
        sampler.start()
        result["phases"]["first_read"] = timed_read(benchmark_root, sources)
        _, first_read_metrics = finish_phase(before_first_read)
        before_warm_read = sampler.sample()
        sampler.start()
        result["phases"]["warm_read"] = timed_read(benchmark_root, sources)
        _, warm_read_metrics = finish_phase(before_warm_read)
        before_delete = sampler.sample()
        sampler.start()
        result["phases"]["delete"] = timed_delete(benchmark_root, sources)
        final, delete_metrics = finish_phase(before_delete)
        result["metrics"] = {
            "write": write_metrics,
            "first_read": first_read_metrics,
            "warm_read": warm_read_metrics,
            "delete": delete_metrics,
            "whole_run": summarize_metrics(baseline, final, whole_maxima),
            "scrape_errors": sampler.errors,
        }
        return result
    finally:
        sampler.stop()
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)


def aggregate_target_runs(target, runs):
    def median(values):
        return statistics.median(values)

    phases = {}
    for name in ("write", "first_read", "warm_read", "delete"):
        samples = [run["phases"][name] for run in runs]
        phases[name] = {
            "duration_seconds": median([sample["duration_seconds"] for sample in samples]),
            "bytes": samples[0]["bytes"],
            "throughput_mib_s": median([sample["throughput_mib_s"] for sample in samples]),
            "file_latency_ms": {
                percentile_name: median(
                    [sample["file_latency_ms"][percentile_name] for sample in samples]
                )
                for percentile_name in ("p50", "p95", "p99")
            },
            "run_throughput_mib_s": [sample["throughput_mib_s"] for sample in samples],
        }
    durability_samples = [
        run["phases"].get("durability") for run in runs if run["phases"].get("durability")
    ]
    if durability_samples:
        phases["durability"] = {
            "duration_seconds": median(
                [sample["duration_seconds"] for sample in durability_samples]
            ),
            "max_failed_uploads": max(
                sample["response"]["failedUploads"] for sample in durability_samples
            ),
            "max_pending_blocks": max(
                sample["response"]["pendingBlocks"] for sample in durability_samples
            ),
            "max_pending_bytes": max(
                sample["response"]["pendingBytes"] for sample in durability_samples
            ),
        }
    scrape_errors = sorted(
        {
            error
            for run in runs
            for error in run.get("metrics", {}).get("scrape_errors", [])
        }
    )
    object_requests = {
        method: median(
            [run["metrics"]["whole_run"]["object_requests"].get(method, 0) for run in runs]
        )
        for method in ("PUT", "GET", "DELETE")
    }
    object_request_errors = max(
        sum(
            value
            for series, value in run["metrics"]["whole_run"]["counter_deltas"].items()
            if series.split("{", 1)[0] == "juicefs_object_request_errors"
        )
        for run in runs
    )
    return {
        "name": target["name"],
        "mount": target["mount"],
        "block_size_mib": target["block_size_mib"],
        "estimated_data_objects": runs[0]["estimated_data_objects"],
        "phases": phases,
        "metrics": {
            "median_object_requests": object_requests,
            "max_object_request_errors": object_request_errors,
            "scrape_errors": scrape_errors,
        },
        "runs": runs,
    }


def choose_recommendation(results):
    baseline = next((item for item in results if item["block_size_mib"] == 4), None)
    if baseline is None:
        return {"selected": None, "reason": "4 MiB baseline missing"}

    def safety_failure(item):
        metrics = item.get("metrics", {})
        if metrics.get("scrape_errors"):
            return "metrics scrape failed"
        if metrics.get("max_object_request_errors", 0) > 0:
            return "object requests failed"
        observed_puts = metrics.get("median_object_requests", {}).get("PUT", 0)
        expected_puts = item.get("estimated_data_objects", 0)
        if not observed_puts or abs(observed_puts - expected_puts) > max(
            1, expected_puts * 0.05
        ):
            return "observed PUT count does not match the expected block split"
        durability = item.get("phases", {}).get("durability")
        if durability and (
            durability.get("max_failed_uploads", 0) > 0
            or durability.get("max_pending_blocks", 0) > 0
            or durability.get("max_pending_bytes", 0) > 0
        ):
            return "durability barrier did not finish cleanly"
        return None

    if failure := safety_failure(baseline):
        return {"selected": None, "reason": f"4 MiB baseline invalid: {failure}"}
    candidates = []
    for item in results:
        if safety_failure(item):
            continue
        phases = item["phases"]
        if any(name not in phases for name in ("write", "first_read", "warm_read")):
            continue
        if phases["write"]["throughput_mib_s"] < baseline["phases"]["write"]["throughput_mib_s"] * 0.90:
            continue
        if phases["first_read"]["throughput_mib_s"] < baseline["phases"]["first_read"]["throughput_mib_s"] * 0.90:
            continue
        if phases["write"]["file_latency_ms"]["p99"] > baseline["phases"]["write"]["file_latency_ms"]["p99"] * 1.20:
            continue
        candidates.append(item)
    if not candidates:
        return {
            "selected": baseline["name"],
            "reason": "all larger candidates failed safety or crossed a rollback threshold",
        }
    selected = min(candidates, key=lambda item: item["estimated_data_objects"])
    if selected["block_size_mib"] == 4:
        return {
            "selected": selected["name"],
            "reason": "larger blocks crossed a throughput or p99 rollback threshold; retain the baseline",
        }
    return {
        "selected": selected["name"],
        "reason": "fewest estimated objects among targets within throughput and p99 rollback thresholds",
    }


def markdown_report(report):
    lines = [
        "# Plori immutable-chunk benchmark",
        "",
        f"Corpus: {report['corpus']['chunks']} chunks, {report['corpus']['bytes']} bytes; "
        f"median of {report['repetitions']} rotated repetitions",
        "",
        "| Target | Block | Objects | Write MiB/s | First read MiB/s | Warm read MiB/s | Write p99 ms | Delete p99 ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in report["results"]:
        phases = item["phases"]
        lines.append(
            f"| {item['name']} | {item['block_size_mib']} MiB | {item['estimated_data_objects']} | "
            f"{phases['write']['throughput_mib_s']:.2f} | {phases['first_read']['throughput_mib_s']:.2f} | "
            f"{phases['warm_read']['throughput_mib_s']:.2f} | "
            f"{phases['write']['file_latency_ms']['p99']:.2f} | {phases['delete']['file_latency_ms']['p99']:.2f} |"
        )
    lines.extend(
        [
            "",
            f"Recommendation: **{report['recommendation']['selected']}** — {report['recommendation']['reason']}.",
            "",
            "The recommendation is valid only for the measured environment; retain the JSON evidence and apply the documented production canary gates.",
        ]
    )
    return "\n".join(lines) + "\n"


def command_version(executable):
    completed = subprocess.run(
        [executable, "version"], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip() or completed.stderr.strip()


def main():
    script_dir = pathlib.Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=pathlib.Path, default=script_dir / "corpus.json")
    parser.add_argument("--targets", type=pathlib.Path, required=True)
    parser.add_argument("--report-json", type=pathlib.Path, required=True)
    parser.add_argument("--report-md", type=pathlib.Path, required=True)
    parser.add_argument("--juicefs", default="juicefs")
    parser.add_argument("--durability-timeout", default="10m")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    parser.add_argument("--work-dir", type=pathlib.Path)
    args = parser.parse_args()

    corpus = load_corpus(args.corpus.expanduser())
    if args.repetitions <= 0:
        parser.error("--repetitions must be positive")
    if args.cooldown_seconds < 0:
        parser.error("--cooldown-seconds must not be negative")
    sizes = corpus["sizes_bytes"]
    targets = load_targets(args.targets.expanduser())
    juicefs_version = command_version(args.juicefs)
    args.report_json = args.report_json.expanduser()
    args.report_md = args.report_md.expanduser()
    for report_path in (args.report_json, args.report_md):
        if not report_path.resolve().parent.is_dir():
            parser.error(f"report directory does not exist: {report_path.parent}")
    work_dir = args.work_dir.expanduser().resolve() if args.work_dir else None
    if work_dir is not None and not work_dir.is_dir():
        parser.error(f"work directory does not exist: {work_dir}")
    temporary_parent = str(work_dir) if work_dir else None
    with tempfile.TemporaryDirectory(prefix="plori-corpus-", dir=temporary_parent) as directory:
        sources = generate_sources(pathlib.Path(directory), sizes)
        runs_by_name = {target["name"]: [] for target in targets}
        for repetition in range(args.repetitions):
            for offset in range(len(targets)):
                target = targets[(repetition + offset) % len(targets)]
                run = run_target(
                    target,
                    sources,
                    sizes,
                    args.juicefs,
                    args.durability_timeout,
                )
                run["repetition"] = repetition + 1
                runs_by_name[target["name"]].append(run)
                if args.cooldown_seconds:
                    time.sleep(args.cooldown_seconds)
        results = [
            aggregate_target_runs(target, runs_by_name[target["name"]])
            for target in targets
        ]
    report = {
        "schema_version": 1,
        "generated_at_unix": int(time.time()),
        "repetitions": args.repetitions,
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "juicefs": juicefs_version,
        },
        "corpus": {"name": corpus["name"], "chunks": len(sizes), "bytes": sum(sizes)},
        "results": results,
        "recommendation": choose_recommendation(results),
    }
    args.report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.report_md.write_text(markdown_report(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
