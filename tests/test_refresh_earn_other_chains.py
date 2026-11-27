from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import build_earn_subaccount_history
from scan_earn_netflow import CHAINS, LOG_DEPOSIT
from scripts import refresh_earn_other_chains as refresh
from scripts.earn_berachain_rpc import RangeLimited


CHAIN = 'ethereum'
START = CHAINS[CHAIN]['start_block']
BASELINE = START + 30
TARGET = BASELINE + 5
OWNER = '0x' + 'a' * 40
NEW_OWNER = '0x' + 'b' * 40
DATE = '2026-11-27'


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content))


def log(block_number, *, changed=False):
    return {'address': CHAINS[CHAIN]['margin'].lower(), 'removed': False,
            'blockNumber': hex(block_number), 'blockHash': f'0x{block_number:064x}',
            'transactionIndex': '0x1', 'transactionHash': '0x' + 'c' * 64,
            'logIndex': '0x2', 'topics': [LOG_DEPOSIT, '0x' + '0' * 24 + OWNER[2:]],
            'data': '0x' + ''.join(f'{n:064x}' for n in (1, 3, 1, 19 if changed else 18, 1, 18))}


def rpc(url, method, params):
    if method == 'eth_chainId':
        return '0x1'
    if method == 'eth_blockNumber':
        return hex(TARGET + 2)
    if method == 'eth_getBlockByNumber':
        number = int(params[0], 16)
        return {'number': hex(number), 'hash': f'0x{number:064x}', 'timestamp': hex(1795773000 + number - TARGET)}
    if method == 'eth_getLogs':
        first, last = int(params[0]['fromBlock'], 16), int(params[0]['toBlock'], 16)
        return [log(BASELINE + 1)] if first <= BASELINE + 1 <= last else []
    raise AssertionError(method)


class EarnOtherChainsTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        snapshots = self.root / 'data/earn-snapshots'
        write(snapshots / 'manifest.json', {'dates': [DATE], 'chains': {DATE: [CHAIN]}})
        write(snapshots / f'{DATE}.json', {'date': DATE,
              'chainMetadata': {CHAIN: {'blockNumber': TARGET, 'interestIndexes': {'3': {}}}},
              'snapshots': {CHAIN: {OWNER: {}, NEW_OWNER: {}}}})
        history = self.root / 'data/earn-subaccount-history'
        self.manifest = history / 'manifest.json'
        write(self.manifest, {'chains': {CHAIN: {'lastBlock': BASELINE, 'fromBlock': START,
              'addressCount': 1, 'selectionAddressCount': 1}, 'arbitrum': {'addressCount': 123}}})
        self.owner_history = history / CHAIN / f'{OWNER}.json'
        write(self.owner_history, {'address': OWNER, 'chain': CHAIN, 'version': 1,
              'lastScannedBlock': BASELINE, 'scanRange': {'fromBlock': START, 'toBlock': BASELINE},
              'accounts': {}, 'summary': {}})
        patcher = patch.object(build_earn_subaccount_history, 'SNAPSHOT_DIR', snapshots)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_replay(self, *, read=rpc, max_blocks=None):
        with patch.object(refresh, 'call', side_effect=read), redirect_stdout(StringIO()) as output:
            refresh.refresh(self.root, CHAIN, max_blocks=max_blocks)
        return output.getvalue()

    def test_replays_existing_history_and_reports_unbootstrapped_snapshot_wallets(self):
        output = self.run_replay()
        published = json.loads(self.owner_history.read_text())
        self.assertEqual(published['lastScannedBlock'], TARGET)
        self.assertEqual(published['scanRange'], {'fromBlock': START, 'toBlock': TARGET})
        self.assertEqual(published['summary']['eventCount'], 1)
        self.assertEqual(published['sourceMetadata']['latestSnapshotDate'], DATE)
        self.assertIn('1 active snapshot wallets lack a baseline', output)
        self.assertFalse((self.owner_history.parent / f'{NEW_OWNER}.json').exists())
        self.assertEqual(json.loads(self.manifest.read_text())['chains']['arbitrum'], {'addressCount': 123})

    def test_bounded_catchup_advances_only_through_verified_intermediate_block(self):
        output = self.run_replay(max_blocks=2)
        payload = json.loads(self.owner_history.read_text())
        self.assertEqual(payload['lastScannedBlock'], BASELINE + 2)
        self.assertEqual(json.loads(self.manifest.read_text())['chains'][CHAIN]['lastBlock'], BASELINE + 2)
        self.assertIn('1 existing histories remain behind', output)
        self.run_replay()
        payload = json.loads(self.owner_history.read_text())
        self.assertEqual(payload['lastScannedBlock'], TARGET)
        self.assertEqual(payload['summary']['eventCount'], 1)

    def test_mismatched_archive_evidence_aborts_without_advancing_history(self):
        before = (self.manifest.read_bytes(), self.owner_history.read_bytes())

        def disagree(url, method, params):
            result = rpc(url, method, params)
            if method == 'eth_getLogs' and result and 'mevblocker' in url:
                return [log(BASELINE + 1, changed=True)]
            return result

        with self.assertRaisesRegex(ValueError, 'disagree'):
            self.run_replay(read=disagree)
        self.assertEqual((self.manifest.read_bytes(), self.owner_history.read_bytes()), before)

    def test_provider_span_limits_split_into_contiguous_verified_ranges(self):
        requested = defaultdict(list)

        def limited(url, method, params):
            if method == 'eth_getLogs':
                first, last = int(params[0]['fromBlock'], 16), int(params[0]['toBlock'], 16)
                if last - first >= 2:
                    raise RangeLimited()
                requested[url].append((first, last))
            return rpc(url, method, params)

        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(refresh, 'call', side_effect=limited):
            observed = refresh.QuorumReader(CHAIN, pool).logs(BASELINE + 1, TARGET)
        self.assertEqual(observed, [log(BASELINE + 1)])
        self.assertEqual(len(requested), 2)
        for source in requested.values():
            self.assertEqual([n for first, last in source for n in range(first, last + 1)],
                             list(range(BASELINE + 1, TARGET + 1)))


if __name__ == '__main__':
    unittest.main()
