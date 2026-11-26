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

    def test_private_rpc_checks_each_active_window_and_quiet_ranges(self):
        reader = object.__new__(ArchiveReader)
        reader.pool = type('Pool', (), {'map':staticmethod(map)})
        reader.url = 'private'
        rows = [dict(blockNumber=hex(111),blockHash='a',transactionIndex='0x0',
                     transactionHash='b',logIndex='0x1',topics=['c'],data='d')]
        private = []
        def read(source, lo, hi):
            if source == 'private':
                private.append((lo,hi))
            return [r for r in rows if lo<=int(r['blockNumber'],16)<=hi]
        reader.read = read
        with patch('scripts.refresh_earn_berachain.time.sleep'):
            self.assertEqual(reader.logs(101,1102),rows)
        self.assertEqual(private,[(101,110),(111,120),(1101,1102)])

    def test_mismatching_log_contents_are_rejected(self):
        from scripts.refresh_earn_berachain import log_signature
        row = dict(blockNumber='0x1',blockHash='a',transactionIndex='0x0',transactionHash='b',
                   logIndex='0x1',topics=['c'],data='0x00')
        self.assertNotEqual(log_signature([row]),log_signature([{**row,'data':'0x01'}]))

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
