import io
from pathlib import Path
import tempfile
import unittest
import sys
import zipfile
from unittest.mock import patch
import json

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recovery_borrow_pnl import ACTIVITY, PARENT, expand, needs_collection, stage


class PublicPnlBundleTests(unittest.TestCase):
    def test_expired_input_collects_again_and_newer_verified_source_is_reused(self):
        def pack(stamp):
            stream=io.BytesIO()
            with zipfile.ZipFile(stream, 'w') as archive:
                archive.writestr(PARENT,json.dumps({'sourceAsOf':stamp}))
            return stream.getvalue()
        now=100000
        with tempfile.TemporaryDirectory() as work:
            candidate=Path(work)/'verified.zip'
            with patch('recovery_borrow_pnl.previous_file',return_value=pack(now-21601)):
                self.assertTrue(needs_collection(candidate,now))
                candidate.write_bytes(pack(now-30))
                self.assertFalse(needs_collection(candidate,now))
            candidate.write_bytes(pack(now-21601))
            with patch('recovery_borrow_pnl.previous_file',return_value=pack(now-30)):
                self.assertFalse(needs_collection(candidate,now))
            candidate.unlink()
            with patch('recovery_borrow_pnl.previous_file',return_value=None):
                self.assertTrue(needs_collection(candidate,now))
                with self.assertRaisesRegex(ValueError, 'No verified'):
                    stage(candidate, Path(work))

    def test_stale_candidate_keeps_public_bytes_and_never_passes_as_fresh(self):
        shard = ACTIVITY + '/' + 'a'*24 + '/arbitrum-3.json'
        def pack(stamp):
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, 'w') as archive:
                archive.writestr(PARENT, json.dumps({'sourceAsOf':stamp}))
                archive.writestr(shard, '{}')
            return stream.getvalue()
        now = 100000
        old = pack(now-21601)
        with tempfile.TemporaryDirectory() as work:
            candidate = Path(work)/'candidate.zip'
            candidate.write_bytes(pack(now-14401))
            with patch('recovery_borrow_pnl.previous_file', return_value=old), \
                    patch('recovery_borrow_pnl.time.time', return_value=now), \
                    patch('recovery_borrow_pnl.subprocess.run') as run:
                stage(candidate, Path(work))
                self.assertIn('--previously-published', run.call_args.args[0])
            self.assertEqual((Path(work)/'verified-borrow-pnl.zip').read_bytes(), old)

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
