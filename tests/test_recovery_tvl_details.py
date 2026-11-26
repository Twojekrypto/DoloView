import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1] / 'scripts'))
import recovery_tvl_details as publication


class TVLDetailsTests(unittest.TestCase):
    def test_partial_votes_and_missing_history_are_rejected(self):
        cache = json.loads((publication.ROOT / publication.VOTES).read_bytes())
        llama = json.loads((publication.ROOT / publication.LLAMA).read_bytes())
        self.assertIsNotNone(publication.verify(cache, llama))
        broken = copy.deepcopy(cache)
        broken['totals']['onchainVoteFetch']['fetchedWallets'] -= 1
        with self.assertRaises(ValueError):
            publication.verify(broken, llama)
        llama['tvl'] = []
        with self.assertRaises(ValueError):
            publication.verify(cache, llama)
