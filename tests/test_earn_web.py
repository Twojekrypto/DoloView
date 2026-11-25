import gzip
from pathlib import Path
import tempfile
import unittest

from scripts.publish_earn_web import bundle, eligible, groups_to_read, validate_generation


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

    def test_candidate_wallet_shard_must_match_its_chain_index(self):
        day = '2026-11-25'
        chains = ['arbitrum', 'berachain', 'ethereum']
        records = {
            'earn-snapshots/manifest.json': {'chains': {day: chains}},
            f'earn-snapshots/{day}.json': {'date': day, 'snapshots': {c: {'wallet': {}} for c in chains},
                                          'chainMetadata': {c: {'blockNumber': 34} for c in chains}},
            'earn-verified-ledger-shards/manifest.json': {'chains': {}},
            'earn-freshness/status.json': {'chains': dict.fromkeys(chains)},
        }
        for chain in chains:
            path = f'{chain}/28.json'
            records['earn-verified-ledger-shards/manifest.json']['chains'][chain] = {
                'snapshotDate': day, 'shards': {'28': {'path': path, 'addressCount': 1}}}
            records['earn-verified-ledger-shards/' + path] = {
                'chain': chain, 'prefix': '28', 'snapshotDate': day, 'ledgers': {'0x28': {}}}
        validate_generation(day, records.__getitem__)
        records['earn-verified-ledger-shards/arbitrum/28.json']['snapshotDate'] = '2026-11-24'
        with self.assertRaisesRegex(ValueError, 'disagrees'):
            validate_generation(day, records.__getitem__)
