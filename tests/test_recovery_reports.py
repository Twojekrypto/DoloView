import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recovery_reports import FIXED, unpack, refresh_reward_quote

class ReportBundleTests(unittest.TestCase):
    def test_reports_can_refresh_without_a_previous_pages_quote(self):
        with patch('recovery_reports.refresh_group',return_value=True) as collect, patch('recovery_reports.previous_file',side_effect=AssertionError):
            refresh_reward_quote(Path('/example'))
        self.assertEqual(collect.call_args.args[1],['dolo_price.json'])

    def test_stale_fallback_quote_blocks_estimates(self):
        with tempfile.TemporaryDirectory() as work, patch('recovery_reports.refresh_group',return_value=False), patch('recovery_reports.previous_file',return_value=b'{}'), patch('recovery_reports.check_freshness',side_effect=ValueError('stale')):
            with self.assertRaisesRegex(ValueError,'stale'):
                refresh_reward_quote(Path(work))
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
