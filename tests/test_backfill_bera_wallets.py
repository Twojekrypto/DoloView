from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scan_earn_netflow import LOG_DEPOSIT
from scripts import backfill_bera_wallets as bf
from scripts.recovery_bera_flow import apply_logs


class BeraWalletBackfillTest(unittest.TestCase):
    def test_prefix_is_shortened_only_for_an_owner_absent_from_an_earlier_complete_scan(self):
        prior = (9000000, {'newer','old'}); seed = {'lastBlock':8000000,'netflows':{'old':{}}}
        self.assertEqual(bf.verified_start('new',prior,seed),9000001)
        self.assertEqual(bf.verified_start('newer',prior,seed),8000001)
        self.assertEqual(bf.verified_start('old',prior,seed),bf.START)

    def test_unreconciled_wallet_is_never_materialized(self):
        address = '0x' + '9'*40
        snapshot = {'chainMetadata':{'berachain':{'blockNumber':8000001}},'snapshots':{'berachain':{address:{}}}}
        flow = {'lastBlock':8000001,'netflows':{address:{'0':{'t':'10'}}}}
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            bf.backfill(root,snapshot,'2026-11-26',flow,(8000000,set()),{'lastBlock':7000000,'netflows':{}},
                        apply_logs,read=lambda *args:[])
            self.assertFalse((root/'data/earn-subaccount-history/berachain'/(address+'.json')).exists())

    def test_disagreeing_full_indexes_never_certify_the_history(self):
        row={'address':bf.MARGIN,'blockNumber':hex(bf.START),'blockHash':'0xa','transactionIndex':'0x0',
             'transactionHash':'0xb','logIndex':'0x0','topics':[LOG_DEPOSIT],'data':'0x'}
        def source(provider, owners, start, end):
            return [row] if provider==bf.INDEX else []
        with patch.object(bf,'owner_logs',side_effect=source), patch.object(bf.time,'sleep'):
            with self.assertRaisesRegex(ValueError,'disagree'):
                bf.corroborate(['0x'+'1'*40],bf.START,bf.START+5)
