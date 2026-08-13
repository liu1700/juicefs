#!/usr/bin/env python3
import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("plori_benchmark", ROOT / "benchmark.py")
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class PloriBenchmarkTest(unittest.TestCase):
    def test_checked_in_corpus_and_object_counts(self):
        corpus = BENCHMARK.load_corpus(ROOT / "corpus.json")
        sizes = corpus["sizes_bytes"]
        self.assertEqual(sum(sizes), 512 * 1024 * 1024)
        self.assertEqual(len(sizes), 131)
        self.assertEqual(BENCHMARK.estimated_objects(sizes, 4), 202)
        self.assertEqual(BENCHMARK.estimated_objects(sizes, 8), 137)
        self.assertEqual(BENCHMARK.estimated_objects(sizes, 16), 131)

    def test_prometheus_parser_and_summary(self):
        before = BENCHMARK.parse_prometheus(
            'juicefs_object_request_durations_histogram_seconds_count{method="PUT",storage_class=""} 10\n'
            "juicefs_blockcache_hits 4\n"
            'node_disk_written_bytes_total{device="nvme0n1"} 1000\n'
            "unrelated_metric 99\n"
        )
        after = BENCHMARK.parse_prometheus(
            'juicefs_object_request_durations_histogram_seconds_count{method="PUT",storage_class=""} 17\n'
            'juicefs_object_request_data_bytes{method="PUT",storage_class=""} 4096\n'
            "juicefs_blockcache_hits 9\n"
            "juicefs_writeback_pending_bytes 12\n"
            'juicefs_process_resident_memory_bytes{mp="/jfs"} 1048576\n'
            'node_disk_written_bytes_total{device="nvme0n1"} 9192\n'
        )
        summary = BENCHMARK.summarize_metrics(
            before, after, {"juicefs_writeback_pending_bytes": 20}
        )
        self.assertEqual(summary["object_requests"]["PUT"], 7)
        self.assertEqual(summary["object_bytes"]["PUT"], 4096)
        self.assertEqual(summary["counter_deltas"]["juicefs_blockcache_hits"], 5)
        self.assertEqual(
            summary["counter_deltas"]['node_disk_written_bytes_total{device="nvme0n1"}'],
            8192,
        )
        self.assertEqual(summary["max_gauges"]["juicefs_writeback_pending_bytes"], 20)

    def test_local_smoke_run_preserves_contents_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source_root = root / "sources"
            mount = root / "mount"
            mount.mkdir()
            sizes = [4096, 65537, 1024 * 1024]
            sources = BENCHMARK.generate_sources(source_root, sizes)
            result = BENCHMARK.run_target(
                {"name": "local", "mount": str(mount), "block_size_mib": 4},
                sources,
                sizes,
                "juicefs",
                "1s",
                skip_durability=True,
            )
            self.assertEqual(result["phases"]["write"]["bytes"], sum(sizes))
            self.assertEqual(result["phases"]["first_read"]["bytes"], sum(sizes))
            self.assertEqual(result["phases"]["warm_read"]["bytes"], sum(sizes))
            self.assertEqual(list(mount.iterdir()), [])
            aggregate = BENCHMARK.aggregate_target_runs(
                {"name": "local", "mount": str(mount), "block_size_mib": 4},
                [result, result, result],
            )
            self.assertEqual(aggregate["phases"]["write"]["bytes"], sum(sizes))
            self.assertEqual(len(aggregate["runs"]), 3)

    def test_source_generation_accepts_existing_work_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            sources = BENCHMARK.generate_sources(pathlib.Path(directory), [1024])
            self.assertEqual(sources[0][2], 1024)

    def test_target_config_rejects_duplicate_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            mount = root / "mount"
            mount.mkdir()
            config = root / "targets.json"
            config.write_text(
                json.dumps(
                    {
                        "targets": [
                            {
                                "name": "same",
                                "mount": str(mount),
                                "block_size_mib": 4,
                                "metrics_url": "http://127.0.0.1:9567/metrics",
                            },
                            {
                                "name": "same",
                                "mount": str(mount),
                                "block_size_mib": 8,
                                "metrics_url": "http://127.0.0.1:9568/metrics",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate"):
                BENCHMARK.load_targets(config, allow_unmounted=True)

    def test_reads_block_size_from_mounted_config_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            mount = pathlib.Path(directory)
            (mount / ".config").write_text(
                json.dumps({"Format": {"BlockSize": 16384}}), encoding="utf-8"
            )
            self.assertEqual(BENCHMARK.read_mount_block_size_mib(mount), 16)

    def test_target_config_requires_http_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = root / "targets.json"
            config.write_text(
                json.dumps(
                    {
                        "targets": [
                            {
                                "name": "unsafe",
                                "mount": str(root),
                                "block_size_mib": 4,
                                "metrics_url": "file:///etc/passwd",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "HTTP"):
                BENCHMARK.load_targets(config, allow_unmounted=True)

    def test_recommendation_retains_baseline_on_regression(self):
        def result(name, block_size, objects, throughput, p99):
            phase = {
                "throughput_mib_s": throughput,
                "file_latency_ms": {"p99": p99},
            }
            return {
                "name": name,
                "block_size_mib": block_size,
                "estimated_data_objects": objects,
                "phases": {"write": phase, "first_read": phase, "warm_read": phase},
                "metrics": {
                    "median_object_requests": {"PUT": objects},
                    "max_object_request_errors": 0,
                    "scrape_errors": [],
                },
            }

        recommendation = BENCHMARK.choose_recommendation(
            [
                result("4m", 4, 202, 100, 10),
                result("8m", 8, 137, 80, 9),
                result("16m", 16, 131, 95, 13),
            ]
        )
        self.assertEqual(recommendation["selected"], "4m")
        self.assertIn("retain", recommendation["reason"])

    def test_recommendation_rejects_unsafe_baseline(self):
        phase = {
            "throughput_mib_s": 100,
            "file_latency_ms": {"p99": 10},
        }
        recommendation = BENCHMARK.choose_recommendation(
            [
                {
                    "name": "baseline",
                    "block_size_mib": 4,
                    "estimated_data_objects": 202,
                    "phases": {"write": phase, "first_read": phase, "warm_read": phase},
                    "metrics": {"max_object_request_errors": 1},
                }
            ]
        )
        self.assertIsNone(recommendation["selected"])
        self.assertIn("invalid", recommendation["reason"])


if __name__ == "__main__":
    unittest.main()
