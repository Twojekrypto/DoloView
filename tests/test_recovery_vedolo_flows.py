import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from recovery_vedolo_flows import FILE, synchronize, validate
from tempfile import TemporaryDirectory


class RecoveryVeDoloFlowsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = (ROOT / FILE).read_bytes()
        cls.seed = validate(cls.raw)

    def test_rejects_missing_finalized_event_and_bad_count(self):
        doc = copy.deepcopy(self.seed)
        doc['locks'] = [row for row in doc['locks']
                        if int(row['block']) > doc['target_block'] - 256]
        with self.assertRaises(ValueError):
            validate(json.dumps(doc), self.seed)
        doc['total_locks'] = len(doc['locks'])
        with self.assertRaises((RuntimeError, ValueError)):
            validate(json.dumps(doc), self.seed)

    def test_stage_keeps_newer_published_block(self):
        doc = copy.deepcopy(self.seed)
        doc['target_block'] += 1
        newer = json.dumps(doc).encode()
        with TemporaryDirectory() as directory:
            candidate = Path(directory) / 'candidate.json'
            candidate.write_bytes(self.raw)
            with patch('recovery_vedolo_flows.previous_file', return_value=newer):
                synchronize(candidate, Path(directory), 'stage')
            self.assertEqual((Path(directory) / FILE).read_bytes(), newer)


if __name__ == '__main__':
    unittest.main()
