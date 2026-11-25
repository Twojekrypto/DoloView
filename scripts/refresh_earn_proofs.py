"""Advance strict Earn proofs only when the canonical scan covers the snapshot block."""
import json
from pathlib import Path
import subprocess
import sys


def ready_chains(metadata, canonical, chains):
    ready = []
    for chain in chains:
        block = int(metadata[chain]['blockNumber'])
        scanned = int(canonical['chains'][chain]['lastBlock'])
        if block <= 0 or scanned <= 0:
            raise ValueError(f'Invalid canonical or snapshot block for {chain}')
        if scanned >= block:
            ready.append(chain)
        else:
            print(f'{chain}: verified scan through {scanned:,}, snapshot at {block:,}. '
                  'Retaining earlier proofs at their original date and block.')
    return ready


def refresh(root):
    snapshots = root/'data/earn-snapshots'
    date = max(json.loads((snapshots/'manifest.json').read_bytes())['dates'])
    metadata = json.loads((snapshots/(date+'.json')).read_bytes())['chainMetadata']
    canonical = json.loads((root/'data/earn-subaccount-history/manifest.json').read_bytes())
    for chain in ready_chains(metadata, canonical, ('arbitrum','berachain','ethereum')):
        subprocess.run([sys.executable,'build_earn_resolved_interest_ledger.py','--chain',chain,
                        '--existing-addresses'],cwd=root,check=True)
    # The public ledger builder independently requires both the same date and
    # the exact pinned block before attaching any of these proofs to a wallet.


if __name__ == '__main__':
    refresh(Path.cwd())
