from datetime import datetime, timezone
import unittest

from scripts.recovery_cadence import due


class RecoveryCadenceTests(unittest.TestCase):
    def test_waits_for_active_work_and_limits_attempts_even_after_a_failure(self):
        now = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
        old = {'status':'completed', 'createdAt':'2026-11-25T10:00:00Z'}
        self.assertTrue(due([old], 2, now))
        self.assertFalse(due([], 2, now))
        self.assertFalse(due([old, {**old, 'status':'queued'}], 2, now))
        self.assertFalse(due([old, {**old, 'createdAt':'2026-11-25T10:03:00Z'}], 2, now))

    def test_failure_retries_earlier_with_bounded_backoff(self):
        now = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
        failed = {'status': 'completed', 'conclusion': 'failure', 'createdAt': '2026-11-25T11:30:00Z'}
        older_failure = {**failed, 'createdAt': '2026-11-25T10:50:00Z'}
        self.assertTrue(due([failed], 2, now))
        self.assertFalse(due([failed, older_failure], 2, now))
        self.assertFalse(due([{**failed, 'createdAt': '2026-11-25T11:31:00Z'}], 12, now))
        self.assertTrue(due([{**failed, 'conclusion': 'timed_out'}], 12, now))
