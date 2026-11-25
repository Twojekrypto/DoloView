import gzip
from pathlib import Path
import tempfile
import unittest

from scripts.publish_earn_web import bundle, eligible, groups_to_read


class PublicEarnTests(unittest.TestCase):
    def test_only_public_runtime_data(self):
        for name in ['earn-snapshots/2026-10', 'earn-subaccount-history/arbitrum/a', 'earn-resolved-interest-ledger/ethereum/e']:
            self.assertFalse(groups_to_read(name))
        self.assertFalse(eligible(Path('data/earn-snapshots/2026-11-24.json')))
        self.assertFalse(eligible(Path('data/earn-verified-ledger-shards/arbitrum/private.json')))
        self.assertTrue(eligible(Path('data/earn-snapshots/2026-11-25.json')))
        for path in ('data/earn-snapshots/hidden/2026-11-25.json',
                     'data/earn-verified-ledger-shards/other/ff.json',
                     'data/earn-verified-ledger-shards/ethereum/manifest.json',
                     'data/earn-snapshots/zzzzzzzzzz.json'):
            self.assertFalse(eligible(Path(path)),path)

    def test_byte_ranges_restore_individual_original_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            paths = [Path('01.json'), Path('02.json')]
            for i, p in enumerate(paths):
                (root/p).write_text('{"value":%d}' % i)
            blob, index = bundle(paths, root)
            for path in paths:
                start, size = index['/' + path.as_posix()]
                self.assertEqual(gzip.decompress(blob[start:start+size]), (root/path).read_bytes())
