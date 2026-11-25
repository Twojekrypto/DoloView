import io
from pathlib import Path
import tempfile
import unittest
import sys
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recovery_borrow_pnl import ACTIVITY, PARENT, expand


class PublicPnlBundleTests(unittest.TestCase):
    def test_only_parent_and_immutable_activity_are_public(self):
        shard = ACTIVITY + '/' + 'a'*24 + '/arbitrum-3.json'
        for extra in (None, '.cache/borrow-position-pnl/a.json', '../outside'):
            raw = io.BytesIO()
            with zipfile.ZipFile(raw,'w') as archive:
                archive.writestr(PARENT,b'{}')
                archive.writestr(shard,b'{}')
                if extra:
                    archive.writestr(extra,b'{}')
            with tempfile.TemporaryDirectory() as work:
                root=Path(work)
                if extra:
                    with self.assertRaises(ValueError): expand(raw.getvalue(),root)
                    self.assertFalse(any(root.iterdir()))
                else:
                    expand(raw.getvalue(),root)
                    self.assertEqual((root/shard).read_bytes(),b'{}')
