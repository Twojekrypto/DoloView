import hashlib
import io
import json
import tarfile
import unittest
from scripts.restore_archived_lp import extract_snapshot


def fixture(stored=b'{"version":1}', manifest_size=None):
    digest = hashlib.sha256(b'{"version":1}').hexdigest()
    manifest = json.dumps({"sha256": digest, "size": len(stored) if manifest_size is None else manifest_size}).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:xz") as pack:
        for folder, data in [("objects", stored), ("versions", manifest)]:
            item = tarfile.TarInfo(f"lp/v1/{folder}/{digest}.json")
            item.size = len(data)
            pack.addfile(item, io.BytesIO(data))
    archive = buffer.getvalue()
    return archive, hashlib.sha256(archive).hexdigest(), digest


class ArchiveTests(unittest.TestCase):
    def test_exact_bytes(self):
        manifest, data = extract_snapshot(*fixture())
        self.assertEqual(len(data), manifest["size"])
        self.assertEqual(data, b'{"version":1}')

    def test_corruption_and_missing_version(self):
        archive, sha, digest = fixture()
        for args in [(archive, "a"*64, digest), (archive, sha, "b"*64),
                     fixture(stored=b'{"version":2}'), fixture(manifest_size=900)]:
            with self.subTest(digest=args[2]), self.assertRaises(ValueError):
                extract_snapshot(*args)
