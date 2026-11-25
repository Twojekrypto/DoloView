import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recovery_reports import FIXED, unpack

class ReportBundleTests(unittest.TestCase):
    def test_only_complete_allowlisted_files_are_extracted(self):
        for names, valid in [(FIXED, True),(FIXED|{'../secret'},False),(set(),False)]:
            data = io.BytesIO()
            with zipfile.ZipFile(data,'w') as archive:
                for n in names: archive.writestr(n,b'{}')
            with tempfile.TemporaryDirectory() as work:
                if valid:
                    unpack(data.getvalue(),Path(work))
                    self.assertTrue(all((Path(work)/name).read_bytes()==b'{}' for name in names))
                else:
                    with self.assertRaises(ValueError): unpack(data.getvalue(),Path(work))
                    self.assertEqual(list(Path(work).iterdir()),[])
