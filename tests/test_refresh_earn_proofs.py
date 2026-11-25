import unittest
from scripts.refresh_earn_proofs import ready_chains
from build_earn_verified_ledger import _validate_resolved_interest_ledger

class PinnedProofTests(unittest.TestCase):
    def test_only_caught_up_chains_can_replace_their_prior_proofs(self):
        meta={c:{'blockNumber':200} for c in ('ethereum','arbitrum','berachain')}
        canonical={'chains':{c:{'lastBlock':n} for c,n in [('arbitrum',200),('ethereum',180),('berachain',201)]}}
        self.assertEqual(ready_chains(meta,canonical,meta),['arbitrum','berachain'])
        self.assertIsNone(_validate_resolved_interest_ledger(
            {'chain':'ethereum','address':'wallet','snapshotDate':'2026-11-24'},
            chain='ethereum',address='wallet',latest_date='2026-11-25',canonical_history={'lastScannedBlock':180},
            snapshot_comparison_block=200))
