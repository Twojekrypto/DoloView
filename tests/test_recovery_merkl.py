from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import recovery_merkl as subject

WALLET = '0x' + 'a' * 40
TOKEN = next(iter(subject.merkl.ELIGIBLE_MARKETS['ethereum']))


class MerklPublicationTests(unittest.TestCase):
    def test_scans_matching_tokens_and_rejects_missing_wallets(self):
        day = datetime.now(timezone.utc).date().isoformat()
        meta = {'addressCount': 2, 'shardCount': 1, 'snapshotDate': day,
                'shards': {'aa': {'addressCount': 2, 'path': 'ethereum/aa.json'}}}
        shard = {'snapshotDate':day,'chain':'ethereum','prefix':'aa','schema':{'market':['token']},
                 'ledgers':{WALLET:[day,{'1':[TOKEN]}],
                            WALLET[:-1]+'1':[day,{'1':['0x'+'b'*40]}]}}
        def read(path):
            return {'chains':{'ethereum':meta}} if path == 'manifest.json' else shard
        with patch.object(subject, 'read_public', side_effect=read):
            self.assertEqual(subject.read_addresses(), [WALLET])
            shard['ledgers'].pop(WALLET)
            with self.assertRaises(ValueError):
                subject.read_addresses()

    def test_bundle_rejects_missing_rewards_and_unexpected_paths(self):
        manifest={'generatedAt':'2026-11-25T00:00:00Z','version':1,'failures':{'ethereum':[]},
                  'chains':{'ethereum':{'eligibleAddressCount':1,'rewardAddressCount':0,
                            'failureCount':0,'markets':subject.merkl.ELIGIBLE_MARKETS['ethereum']}}}
        def packed(extra=False):
            output=io.BytesIO()
            with zipfile.ZipFile(output,'w') as archive:
                archive.writestr(subject.MANIFEST,json.dumps(manifest))
                if extra:
                    archive.writestr('../state','x')
            return output.getvalue()
        self.assertEqual(subject.verified(packed()), manifest['generatedAt'])
        with self.assertRaises(ValueError):
            subject.verified(packed(True))
        manifest['chains']['ethereum']['rewardAddressCount']=1
        with self.assertRaises(ValueError):
            subject.verified(packed())
