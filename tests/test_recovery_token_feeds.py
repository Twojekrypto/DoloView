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


if __name__ == '__main__':
    unittest.main()
