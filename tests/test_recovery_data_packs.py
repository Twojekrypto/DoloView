import json
import tempfile
import unittest
from pathlib import Path

from scripts.recovery_data_packs import pack, restore


class RecoveryDataPackTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.source = self.root / 'source'
        self.paths = {
            'data/earn-snapshots/2026-10-01.json': b'{"date":1}',
            'data/earn-snapshots/2026-11-01.json': b'{"date":2}',
            'data/earn-subaccount-history/berachain/0xa11.json': b'{"events":[]}',
            'data/supply-activity/arbitrum/0x123.part1.json': b'[{"value":1}]',
            'data/supply-activity/arbitrum/0x123.part2.json': b'[{"value":2}]',
        }
        for name, content in self.paths.items():
            path = self.source / name
            path.parent.mkdir(exist_ok=True, parents=True)
            path.write_bytes(content)

    def test_round_trip_preserves_exact_bytes_and_groups_market_parts(self):
        output = self.root / 'packed'
        summary = pack(self.source, output)
        self.assertEqual(len(summary['chunks']), 4)
        self.assertEqual(summary['chunks']['supply-activity/arbitrum/0x123']['files'], 2)
        restored = self.root / 'restored'
        self.assertEqual(restore(output, restored), len(self.paths))
        self.assertEqual({name: (restored / name).read_bytes() for name in self.paths}, self.paths)

    def test_rebuild_writes_only_changed_pack(self):
        baseline = self.root / 'baseline'
        original = pack(self.source, baseline)
        modified = 'data/earn-snapshots/2026-11-01.json'
        (self.source / modified).write_bytes(b'{"date":3}')
        update = self.root / 'update'
        updated = pack(self.source, update, original)
        changed = [key for key in original['chunks'] if original['chunks'][key] != updated['chunks'][key]]
        self.assertEqual(changed, ['earn-snapshots/2026-11'])
        self.assertEqual(len(list((update / 'objects').glob('*.tar.zst'))), 1)

    def test_restore_can_load_one_dataset(self):
        packed = self.root / 'all'
        pack(self.source, packed)
        restored = self.root / 'restored'
        self.assertEqual(restore(packed, restored, datasets=['earn-snapshots']), 2)
        self.assertTrue((restored / 'data/earn-snapshots/2026-11-01.json').exists())
        self.assertFalse((restored / 'data/supply-activity').exists())

    def test_missing_dataset_does_not_replace_previous_manifest(self):
        baseline = pack(self.source, self.root / 'baseline')
        for name in self.paths:
            if name.startswith('data/supply-activity'):
                (self.source / name).unlink()
        update = self.root / 'update'
        with self.assertRaisesRegex(ValueError, 'missing datasets'):
            pack(self.source, update, baseline)
        self.assertFalse(update.exists())

    def test_corrupt_pack_leaves_output_absent(self):
        baseline = self.root / 'baseline'
        pack(self.source, baseline)
        next((baseline / 'objects').glob('*.tar.zst')).write_bytes(b'invalid')
        restored = self.root / 'restored'
        with self.assertRaisesRegex(ValueError, 'checksum'):
            restore(baseline, restored)
        self.assertFalse(restored.exists())


if __name__ == '__main__':
    unittest.main()
