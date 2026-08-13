#!/usr/bin/env python3
import datetime as dt
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("check-govulncheck.py")


class GovulncheckGateTest(unittest.TestCase):
    def run_gate(self, events, waivers):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scan = root / "scan.json"
            waiver_file = root / "waivers.json"
            scan.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")
            waiver_file.write_text(json.dumps({"schemaVersion": 1, "waivers": waivers}), encoding="utf-8")
            return subprocess.run(
                [sys.executable, SCRIPT, scan, waiver_file, "juicefs.plori"],
                check=False,
                capture_output=True,
                text=True,
            )

    @staticmethod
    def complete_scan(*findings):
        return [
            {"config": {"scan_level": "symbol"}},
            {"SBOM": {"go_version": "go1.25.12"}},
            *({"finding": finding} for finding in findings),
        ]

    def test_rejects_incomplete_scan(self):
        result = self.run_gate([{"config": {"scan_level": "symbol"}}], [])
        self.assertEqual(result.returncode, 2)
        self.assertIn("incomplete govulncheck symbol scan", result.stderr)

    def test_rejects_unwaived_symbol_finding(self):
        finding = {"osv": "GO-TEST-1", "trace": [{"function": "Vulnerable"}]}
        result = self.run_gate(self.complete_scan(finding), [])
        self.assertEqual(result.returncode, 1)
        self.assertIn("GO-TEST-1", result.stderr)

    def test_accepts_active_exact_waiver(self):
        finding = {"osv": "GO-TEST-1", "trace": [{"function": "Vulnerable"}]}
        waiver = {
            "id": "GO-TEST-1",
            "artifact": "juicefs.plori",
            "expires": (dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=30)).isoformat(),
            "reason": "test waiver",
        }
        result = self.run_gate(self.complete_scan(finding), [waiver])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_expired_waiver(self):
        waiver = {
            "id": "GO-TEST-1",
            "artifact": "juicefs.plori",
            "expires": "2000-01-01",
            "reason": "expired test waiver",
        }
        result = self.run_gate(self.complete_scan(), [waiver])
        self.assertEqual(result.returncode, 2)
        self.assertIn("expired vulnerability waiver", result.stderr)


if __name__ == "__main__":
    unittest.main()
