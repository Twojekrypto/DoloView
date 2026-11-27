import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts import recovery_token_feeds as feeds


class TokenFeedPublicationTests(unittest.TestCase):
    def test_bundle_rejects_wallet_archive_and_wrong_member(self):
        for extra in ('dolo_holder_wallet_history.json', '../odolo_contract_data.json'):
            with self.subTest(extra=extra):
                data = io.BytesIO()
                with zipfile.ZipFile(data, 'w') as archive:
                    archive.writestr('odolo_contract_data.json', '{}')
                    archive.writestr(extra, '{}')
                with self.assertRaises(ValueError):
                    feeds.unpack('odolo-contract', data.getvalue())

    def test_publisher_retains_original_verified_public_bytes(self):
        name = 'odolo_contract_data.json'
        old = b'{"blockNumber":100,"last_updated":"2026-11-20T00:00:00Z"}'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(feeds, 'get_previous', return_value={name: old}):
                feeds.stage('odolo-contract', root / 'absent.zip', root)
            self.assertEqual((root / name).read_bytes(), old)

    def test_fresh_wrapper_cannot_replace_newer_chain_block(self):
        name = 'odolo_contract_data.json'
        old = {'blockNumber': 100, 'last_updated': '2026-11-20T00:00:00Z'}
        stale_source = {'blockNumber': 99, 'last_updated': '2026-11-20T00:10:00Z'}
        with self.assertRaisesRegex(ValueError, 'regressed'):
            feeds.validate_progress(name, stale_source, old)

    def test_publication_freshness_uses_source_alongside_wrapper(self):
        with self.assertRaisesRegex(ValueError, 'source block'):
            feeds.require_fresh_source('berachain', 100, 86000, timestamp=lambda *_: 70000)

    def test_vedolo_inventory_can_use_verified_history_ahead_of_pages(self):
        published = b'{"target_block":100}'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / 'verified' / feeds.FLOW_INPUT
            candidate.parent.mkdir()
            with patch.object(feeds, 'ROOT', root), patch.object(feeds, 'previous_file', return_value=published), patch.object(
                feeds, 'validate_vedolo_flows', side_effect=lambda raw, *_: json.loads(raw)
            ) as validate:
                for block in (120, 90):
                    candidate.write_text(json.dumps({'target_block': block}))
                    feeds.refresh_verified_vedolo_input(candidate)
                    self.assertEqual(json.loads((root / feeds.FLOW_INPUT).read_bytes())['target_block'], max(block, 100))
                validate.assert_any_call(b'{"target_block": 120}', {'target_block': 100})

    def test_activity_report_requires_all_sources_to_agree(self):
        usd = {'total_txs': 2, 'total_usdc': 30.12}
        duration = {'total_exercises': 4, 'valid_durations': 4, 'distribution': {'< 1 month': 4}}
        rows = {'total_exercises': 4, 'total_odolo_exercised_exercises': 2,
                'total_odolo_exercise_usdc_paid': 30.12}
        documents = {
            'exercised_usd.json': usd, 'avg_lock_data.json': duration,
            'exercisers_by_address.json': rows,
        }
        feeds.validate_activity(documents)
        duration['total_exercises'] = 5
        with self.assertRaisesRegex(ValueError, 'reconcile'):
            feeds.validate_activity(documents)

    def test_rpc_activity_refuses_provider_disagreement(self):
        from generate_dolo_flows import TransferLogQuorumError
        from generate_vedolo_flows import ODOLO_VESTER, TRANSFER_TOPIC

        def client(logs):
            from unittest.mock import Mock
            fake = Mock()
            fake.call.return_value = logs
            return fake

        log = {'address': ODOLO_VESTER, 'blockNumber': '0x64',
               'transactionHash': '0x' + '11' * 32, 'logIndex': '0x5',
               'topics': [TRANSFER_TOPIC, '0x' + '00' * 32, '0x' + '22' * 32,
                          '0x' + '00' * 31 + '01'],
               'data': '0x'}
        family_a = 'https://berachain-rpc.publicnode.com'
        family_b = 'https://berachain.drpc.org'
        with self.assertRaises(TransferLogQuorumError):
            feeds.verified_vester_logs({family_a: client([log]), family_b: client([])}, 100, 100)
        rows, proof = feeds.verified_vester_logs({family_a: client([log]), family_b: client([log])},
                                                 100, 100)
        self.assertEqual(len(rows), 1)
        self.assertEqual(proof['matchingProviderFamilies'], 2)

    def test_vedolo_population_requires_mints_burns_and_transfer_owners(self):
        owner = '0x' + '11' * 20
        next_owner = '0x' + '22' * 20
        flows = {
            'locks': [
                {'depositType': 1, 'tokenId': 1, 'block': 10},
                {'depositType': 5, 'tokenId': 2, 'block': 11},
                {'depositType': 1, 'tokenId': 3, 'block': 13},
            ],
            'unlocks': [{'tokenId': 2, 'block': 12}],
            'transfers': [{'tokenId': 1, 'to': next_owner, 'block': 14, 'logIndex': 1}],
        }
        previous = {'snapshot_block': 12, 'stats': {'nft_transfer_count': 4,
                     'nft_transfer_min_block': 9, 'nft_transfer_max_block': 12}}
        holders = [{'address': next_owner, 'token_ids': [1]},
                   {'address': owner, 'token_ids': [3]}]
        result = feeds.verified_vedolo_population(flows, previous, holders, {1, 2, 3})
        self.assertEqual(result['active_nfts'], 2)
        self.assertEqual(result['total_burned'], 1)
        self.assertEqual(result['nft_transfer_count'], 6)
        self.assertEqual(result['nft_transfer_min_block'], 9)
        holders[0]['address'] = owner
        with self.assertRaisesRegex(ValueError, 'owner'):
            feeds.verified_vedolo_population(flows, previous, holders, {1, 2, 3})

    def test_inventory_reuses_the_verified_block_for_positions_and_fee_quotes(self):
        owner = '0x' + '11' * 20
        timestamp = 1700000000
        holders = [{'address': owner, 'token_ids': [1]}]
        flows = {'target_block': 11, 'locks': [{'depositType': 1, 'tokenId': 1, 'block': 10}],
                 'unlocks': [], 'transfers': []}
        previous = {'snapshot_block': 10, 'stats': {'nft_transfer_count': 1,
                    'nft_transfer_min_block': 10, 'nft_transfer_max_block': 10}}
        positions, quotes = {1: {'amount': '100'}}, {1: {'fee': '10'}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / feeds.FLOW_INPUT).write_text('flow data')
            (root / 'vedolo_holders.json').write_text(json.dumps(previous))
            with patch.object(feeds, 'ROOT', root), \
                    patch.object(feeds, 'validate_vedolo_flows', return_value=flows), \
                    patch.object(feeds, 'require_fresh_source') as freshness, \
                    patch('update_data.VEDOLO_SNAPSHOT_BLOCK', None), \
                    patch('update_data.main') as export_holders, \
                    patch('generate_vedolo_early_unlock.fetch_onchain_inputs',
                          return_value=(holders, positions, quotes, timestamp, 11, owner)) as read_positions, \
                    patch('generate_vedolo_early_unlock.build_simulation_payload', return_value={'sourceBlock': 11}) as simulate, \
                    patch('generate_vedolo_early_unlock.write_simulation_payload') as export_quotes:
                feeds.refresh_vedolo_inventory()
            self.assertEqual(freshness.call_args.args[:2], ('berachain', 11))
            read_positions.assert_called_once_with(11)
            self.assertEqual(export_holders.call_args.args[0][1]['active_nfts'], 1)
            self.assertEqual(export_holders.call_args.args[1:], (positions, timestamp))
            simulate.assert_called_once_with(holders, positions, quotes, snapshot_timestamp=timestamp,
                                             source_block=11, fee_calculator=owner)
            export_quotes.assert_called_once_with({'sourceBlock': 11})


if __name__ == '__main__':
    unittest.main()
