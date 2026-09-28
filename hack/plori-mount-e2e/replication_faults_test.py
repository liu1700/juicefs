"""Checks for the fault harness's event assertions, without FUSE or S3."""

import unittest
from unittest.mock import Mock

from replication_faults import (
    events, first_running_failure, require_no_terminal, require_recovery,
    require_replication_terminal, require_stop_deadlines,
)


class EventAssertions(unittest.TestCase):
    def test_mixed_output_and_partial_json(self):
        path = Mock()
        path.read_text.return_value = (
            'juicefs log\n{"event":"replication_probe_failed"}\n'
            '42\n{"event":"replication_recovered"}\n{"event":'
        )
        self.assertEqual(events(path), [
            {"event": "replication_probe_failed"},
            {"event": "replication_recovered"},
        ])

    def test_survival_rejects_even_clean_terminal_and_error_without_terminal(self):
        require_no_terminal([{"event": "replication_recovered"}])
        for record in (
            {"event": "plori_mount_terminal", "exit": 0},
            {"event": "other", "error": "E_REPLICATION_FAILED"},
        ):
            with self.subTest(record=record), self.assertRaises(AssertionError):
                require_no_terminal([record])

    def test_bounded_pause_requires_exact_terminal_code_and_exit(self):
        terminal = {"event": "plori_mount_terminal", "error": "E_REPLICATION_FAILED", "exit": 69}
        require_replication_terminal([terminal])
        for records in (
            [], [terminal, terminal],
            [dict(terminal, exit=0)], [dict(terminal, error="E_FENCED")],
        ):
            with self.subTest(records=records), self.assertRaises(AssertionError):
                require_replication_terminal(records)

    def test_probe_state_and_count_parsing(self):
        path = Mock()
        path.read_text.return_value = '\n'.join([
            '{"event":"replication_probe_failed","replicator":"starting","probe_failures":0}',
            '{"event":"replication_probe_failed","replicator":"running","probe_failures":1}',
            '{"event":"replication_restarted","reason":"probe_failures"}',
        ])
        records = events(path)
        self.assertIs(first_running_failure(records), records[1])
        self.assertEqual(records[2]["reason"], "probe_failures")
        self.assertIsNone(first_running_failure(records[:1]))
        with self.assertRaises(AssertionError):
            first_running_failure([dict(records[1], probe_failures=2)])

    def test_short_stall_rejects_any_restart(self):
        records = [self.failure(1), {"event": "replication_recovered"}]
        require_recovery(records)
        for event in ("replication_restarted", "replication_restart_failed", "plori_mount_terminal"):
            with self.subTest(event=event), self.assertRaises(AssertionError):
                require_recovery(records + [{"event": event}])
        with self.assertRaises(AssertionError):
            require_recovery(records[:1])

    @staticmethod
    def failure(count, state="running"):
        return {"event": "replication_probe_failed", "replicator": state,
                "probe_failures": count, "ts": "2026-01-01T00:00:00Z"}

    def test_hung_child_requires_three_failures_reason_order_and_deadline(self):
        records = [self.failure(n) for n in (1, 2, 3)] + [
            {"event": "replication_restarted", "reason": "probe_failures",
             "ts": "2026-01-01T00:00:15Z"}, {"event": "replication_recovered"}]
        require_recovery(records, "probe_failures")
        invalid = [
            records[:2] + records[3:],
            records + [records[3]],
            records[:3] + [dict(records[3], reason="gone"), records[4]],
            records[:3] + [dict(records[3], ts="2026-01-01T00:00:21Z"), records[4]],
            records[:3] + [records[4], records[3]],
        ]
        for case in invalid:
            with self.subTest(case=case), self.assertRaises(AssertionError):
                require_recovery(case, "probe_failures")

    def test_crash_requires_gone_and_exactly_one_restart(self):
        records = [self.failure(0, "gone"),
                   {"event": "replication_restarted", "reason": "gone"},
                   {"event": "replication_recovered"}]
        require_recovery(records, "gone")
        for case in (records + [records[1]], records[1:],
                     [self.failure(1)] + records[1:],
                     [records[0], dict(records[1], reason="probe_failures"), records[2]]):
            with self.subTest(case=case), self.assertRaises(AssertionError):
                require_recovery(case, "gone")

    def test_long_pause_bounds_stop_terminal_and_lease(self):
        from replication_faults import timestamp
        first = self.failure(1)
        records = [
            {"event": "replication_failed_stop", "ts": "2026-01-01T00:00:30Z"},
            {"event": "plori_mount_terminal", "error": "E_REPLICATION_FAILED",
             "exit": 69, "ts": "2026-01-01T00:00:40Z"},
        ]
        lease_stop = timestamp(first) + 90
        require_stop_deadlines(records, first, lease_stop)
        for case, deadline in (
            ([dict(records[0], ts="2026-01-01T00:00:36Z"), records[1]], lease_stop),
            ([dict(records[0], ts="2026-01-01T00:00:29Z"), records[1]], lease_stop),
            ([records[0], dict(records[1], ts="2026-01-01T00:01:06Z")], lease_stop),
            (records, timestamp(first) + 35),
        ):
            with self.subTest(case=case, deadline=deadline), self.assertRaises(AssertionError):
                require_stop_deadlines(case, first, deadline)


if __name__ == "__main__":
    unittest.main()
