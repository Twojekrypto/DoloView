import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts import recovery_bera_flow as flow
from scan_earn_netflow import LOG_WITHDRAW

class BeraNetflowTest(unittest.TestCase):
    def record(self, **diff):
        data = dict(chain='berachain', lastBlock=29243078, scanComplete=True,
                    scanStatus='complete', addressCount=1, netflows={'wallet':{'0':{'t':'100'}}})
        return json.dumps(dict(data, **diff)).encode()

    def test_existing_totals_preserved_with_exact_signed_deltas(self):
        doc = flow.checked(self.record())
        log = {'blockHash':'0xa','logIndex':'0x0','topics':[LOG_WITHDRAW]}
        with patch.object(flow, 'decode_log_entries', return_value=[
            {'owner':'wallet','market':'0','delta':-30},
            {'owner':'second','market':'2','delta':20}
        ]):
            flow.apply_logs(doc['netflows'], [log])
            self.assertEqual(doc['netflows']['wallet']['0']['t'], '70')
            self.assertEqual(doc['netflows']['wallet']['0']['w'], '30')
            self.assertEqual(doc['netflows']['second']['2']['t'], '20')
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                flow.apply_logs({}, [log,log])

    def test_refuses_partial_or_unsupported_cycle_baseline(self):
        for raw in (self.record(scanStatus='partial'), self.record(addressCount=2),
                    self.record(netflows={'wallet':{'0':{'t':'10','endingPar':'15'}}})):
            with self.assertRaises(ValueError):
                flow.checked(raw)

    def test_equal_block_conflict_and_no_future_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            candidate=Path(tmp)/'input.json'
            candidate.write_bytes(self.record(lastBlock=29243079))
            with patch.object(flow,'previous_file', return_value=self.record(lastBlock=29243079, netflows={'x':{'1':{'t':'42'}}})):
                with self.assertRaisesRegex(ValueError,'disagree'):
                    flow.baseline(candidate)
                self.assertEqual(flow.baseline(candidate,limit=29243078)['addressCount'],41544)

if __name__ == '__main__':
    unittest.main()
