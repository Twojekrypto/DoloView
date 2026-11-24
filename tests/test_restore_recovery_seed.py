import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.restore_recovery_seed import restore


class RecoverySeedTests(unittest.TestCase):
    def seed(self, root, name, content=b'{}', *, wrong_hash=False):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode='w') as archive:
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            archive.addfile(entry, io.BytesIO(content))
        body = subprocess.check_output(['zstd', '-q', '-c'], input=raw.getvalue())
        parts = [
            ('data.tar.zst.part-aa', body[:8]),
            ('data.tar.zst.part-ab', body[8:]),
        ]
        for filename, data in parts:
            (root / filename).write_bytes(data)
        (root / 'manifest.json').write_text(json.dumps({
            'version': 1, 'format': 'tar.zst-split', 'files': 1,
            'parts': [
                {'name': filename, 'size': len(data),
                 'sha256': ('0' * 64 if wrong_hash else hashlib.sha256(data).hexdigest())}
                for filename, data in parts
            ],
        }))

    def test_verified_parts_restore_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.seed(root, 'data/earn-snapshots/manifest.json', b'{"valid":true}')
            target = root / 'restore'
            restore(root, target)
            self.assertEqual((target / 'data/earn-snapshots/manifest.json').read_bytes(), b'{"valid":true}')

    def test_checksum_mismatch_leaves_no_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.seed(root, 'data/supply-activity/manifest.json', wrong_hash=True)
            target = root / 'restore'
            with self.assertRaisesRegex(ValueError, 'checksum'):
                restore(root, target)
            self.assertFalse(target.exists())

    def test_files_outside_expected_data_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.seed(root, 'data/earn-snapshots/../../unexpected.txt')
            target = root / 'restore'
            with self.assertRaisesRegex(ValueError, 'archive path'):
                restore(root, target)
            self.assertFalse(target.exists())
            self.assertFalse((root / 'unexpected.txt').exists())


if __name__ == '__main__':
    unittest.main()
