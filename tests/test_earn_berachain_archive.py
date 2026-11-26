import os
import unittest
from unittest.mock import patch

from scripts import earn_berachain_rpc as rpc
from scripts.refresh_earn_berachain import ArchiveReader


class BerachainArchiveTest(unittest.TestCase):
    def test_incomplete_archive_is_rejected_even_with_a_valid_chain_id(self):
        def read(url, method, params):
            if method == 'eth_chainId':
                return hex(80094)
            self.assertEqual(method, 'eth_getLogs')
            return []
        with patch.object(rpc,'call',side_effect=read), self.assertRaisesRegex(ValueError,'0/151'):
            rpc.verify('https://example.test')

    def test_free_plan_splits_each_range_without_dropping_the_last_block(self):
        reader = object.__new__(ArchiveReader)
        reader.span = 10
        reader.pool = type('Pool', (), {'map':staticmethod(map)})
        reader.read = lambda bounds: [bounds]
        self.assertEqual(reader.logs(101,123), [(101,110),(111,120),(121,123)])

    def test_supports_both_the_url_and_key_formats(self):
        key = 'x'*32
        url = 'https://berachain-mainnet.g.alchemy.com/v2/'+key
        for value in (url,key):
            with patch.dict(os.environ, {'ALCHEMY_BERACHAIN_RPC':value}):
                self.assertEqual(rpc.endpoint(),url)

    def test_wrong_chain_stops_before_reading_any_events(self):
        with patch.object(rpc,'call',return_value='0x1') as read, self.assertRaisesRegex(ValueError,'wrong chain'):
            rpc.verify('https://example.test')
        self.assertEqual(read.call_count,1)
