import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from scripts import recovery_supply_activity as activity


NOW = datetime.fromtimestamp(1795770600, timezone.utc)
TOKEN = '0x' + 'c' * 40
WALLET = '0x' + 'd' * 40
CHAINS = ('ethereum', 'arbitrum', 'berachain')
BLOCKS = {
    chain: {'hash': '0x' + str(index) * 64, 'number': 9900 + index,
            'timestamp': 1795770600 - 10 * index}
    for index, chain in enumerate(CHAINS, 1)
}


def source_row(serial, timestamp):
    tx = f'0x{serial:064x}'
    return {'id': f'{tx}-1', 'serialId': str(serial), 'token': {'id': TOKEN},
            'effectiveUser': {'id': WALLET}, 'marginAccount': {'id': f'{WALLET}-1'},
            'amountDeltaWei': '123456789.123456789123456789', 'amountUSDDeltaWei': '0',
            'transaction': {'id': tx, 'timestamp': str(timestamp), 'blockNumber': '9899'}}


def graph_fixture(endpoint, body):
    query = body['query']
    block = BLOCKS[endpoint]
    if '_meta' in query:
        if 'hash:' in query and block['hash'] not in query:
            raise AssertionError('Incorrect pinned source block')
        return {'_meta': {'deployment': 'Qm' + ('a' * 44), 'hasIndexingErrors': False, 'block': block}}
    if block['hash'] not in query:
        raise AssertionError('Unpinned source query')
    if 'tokens(' in query:
        return {'tokens': [{'id': TOKEN, 'symbol': 'TOKEN', 'name': 'Token', 'marketId': '4'}]}
    date_bounds = re.search(r'timestamp_gte:\s*"(\d+)".*timestamp_lte:\s*"(\d+)"', query, re.S)
    start, end = map(int, date_bounds.groups())
    cursor_match = re.search(r'id_gt:\s*"([^"]+)"', query)
    cursor = cursor_match[1] if cursor_match else ''
    page_size = int(re.search(r'first:\s*(\d+)', query)[1])
    entity = 'deposits' if 'deposits(' in query else 'withdrawals'
    observations = [(1, 1793178570), (2, 1795770569), (3, 1795770570), (4, 1795770571)]
    if entity == 'withdrawals':
        observations = [(3, 1795770570)]
    rows = [source_row(serial, stamp) for serial, stamp in observations if start <= stamp <= end]
    return {entity: [row for row in rows if row['id'] > cursor][:page_size]}


class RecoverySupplyActivityTests(unittest.TestCase):
    def build(self, graph_response=graph_fixture, *, now=NOW):
        with patch.object(activity, 'GRAPH_ENDPOINTS', dict(zip(CHAINS, CHAINS))), \
                patch.object(activity, 'post_json', graph_response), patch.object(activity, 'PAGE_SIZE', 2):
            payload = activity.build_overview(now=now)
        return activity, payload

    def test_full_window_ends_at_oldest_pinned_indexed_block(self):
        _, payload = self.build()
        self.assertEqual(payload['periodEnd'], 1795770570)
        self.assertEqual(payload['periodStart'], 1793178570)
        self.assertEqual(payload['sourceGeneratedAt'], '2026-11-27T09:09:30Z')
        self.assertEqual([chain['rowCount'] for chain in payload['chains']], [4, 4, 4])
        self.assertEqual([chain['sourceBlock']['number'] for chain in payload['chains']], [9901, 9902, 9903])
        self.assertEqual(payload['coverage']['includedAssets'], 3)
        self.assertEqual([r[0] for r in payload['rows']].count('deposit-3'), 3)
        self.assertEqual([r[0] for r in payload['rows']].count('withdraw-3'), 3)
        self.assertEqual([r[0] for r in payload['rows']].count('deposit-1'), 3)
        self.assertEqual(payload['rows'][0][4:8], ['123456789.123456789123456789', None, WALLET, ''])
        self.assertEqual(payload['rows'][0][9], {'accountNumber': '1', 'accountOrigin': 'other',
                                                'usdStatus': 'unavailable', 'sourceUsd': '0'})

    def test_rejects_a_source_reporting_indexing_errors(self):
        def graph_response(endpoint, body):
            response = graph_fixture(endpoint, body)
            if endpoint == 'berachain' and '_meta' in response:
                response['_meta']['hasIndexingErrors'] = True
            return response

        with self.assertRaisesRegex(ValueError, 'berachain'):
            self.build(graph_response)

    def test_rejects_extra_fields_duplicate_events_and_stale_source_evidence(self):
        activity, payload = self.build()
        fields = copy.deepcopy(payload)
        fields['rows'][0][9]['privateWalletHistory'] = []
        with self.assertRaises(ValueError):
            activity.verify(fields, now=NOW)
        duplicate = copy.deepcopy(payload)
        duplicate['rows'][0] = duplicate['rows'][1]
        with self.assertRaises(ValueError):
            activity.verify(duplicate, now=NOW)
        with self.assertRaises(ValueError):
            activity.verify(payload, now=NOW + timedelta(hours=4))

    def test_stage_promotes_fresh_candidate_without_rolling_back_previous_feed(self):
        activity, payload = self.build()
        older = copy.deepcopy(payload)
        older['sourceGeneratedAt'] = older['generatedAt'] = '2026-11-27T09:09:29Z'
        older['periodStart'] -= 1
        older['periodEnd'] -= 1
        for entry in older['chains']:
            entry['sourceGeneratedAt'] = older['sourceGeneratedAt']
            entry['sourceBlock']['timestamp'] -= 1
        older['rows'] = [row for row in older['rows'] if row[2] <= older['periodEnd']]
        for entry in older['chains']:
            entry['rowCount'] = 2
        old_bytes = json.dumps(older).encode()
        now_bytes = json.dumps(payload).encode()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            candidate = output / 'candidate.json'
            candidate.write_bytes(now_bytes)
            with patch.object(activity, 'previous_public_activity', return_value=old_bytes):
                activity.stage(candidate, output, now=NOW)
            target = output / activity.PUBLIC_PATH
            self.assertEqual(target.read_bytes(), now_bytes)
            candidate.write_bytes(old_bytes)
            with patch.object(activity, 'previous_public_activity', return_value=now_bytes):
                activity.stage(candidate, output, now=NOW)
            self.assertEqual(target.read_bytes(), now_bytes)
            candidate.unlink()
            with patch.object(activity, 'previous_public_activity', return_value=now_bytes):
                activity.stage(candidate, output, now=NOW + timedelta(hours=4))
            self.assertEqual(target.read_bytes(), now_bytes)


if __name__ == '__main__':
    unittest.main()
