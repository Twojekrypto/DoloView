import unittest

from scripts.retain_berachain_caches import retained_and_expired


class CacheRetentionTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"created_at": str(i), "id": i, "key": str(i)} for i in [3, 1, 2]]

    def test_only_older_copies_are_selected(self):
        keep, old = retained_and_expired(self.rows, lambda key: True)
        self.assertEqual([r["id"] for r in keep], [3, 2])
        self.assertEqual([r["id"] for r in old], [1])

    def test_unverified_new_copy_or_incomplete_pair_retains_all(self):
        for key in ["2", "3"]:
            self.assertEqual(retained_and_expired(self.rows, lambda k: k != key), ([], []))
        self.assertEqual(retained_and_expired(self.rows[:2], lambda k: True), ([], []))
