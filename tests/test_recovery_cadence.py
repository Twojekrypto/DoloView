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
