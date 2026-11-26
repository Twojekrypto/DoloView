"""Reconstruct missing Berachain histories from independently checked owner logs."""
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import hashlib
import json
import time

import requests

from build_earn_subaccount_history import (_append_event, _empty_history, _finalize_history,
                                           _sort_logs, _write_histories)
from scan_earn_netflow import ALL_EVENTS, SECOND_OWNER_EVENTS, CHAINS, _addr_topic, _dedupe_logs, decode_log_entries
from scripts.earn_berachain_rpc import RangeLimited, call
from scripts.refresh_earn_berachain import log_signature

CHAIN = 'berachain'
MARGIN = CHAINS[CHAIN]['margin'].lower()
START = CHAINS[CHAIN]['start_block']
OFFICIAL = 'https://rpc.berachain.com/'
INDEX = 'https://berachain.gateway.tenderly.co'


def owner_logs(provider, owners, start, end):
    rows = []
    for topics in ([ALL_EVENTS, owners], [SECOND_OWNER_EVENTS, None, owners]):
        query = {'fromBlock': hex(start), 'toBlock': hex(end), 'address': MARGIN, 'topics': topics}
        try:
            batch = call(provider, 'eth_getLogs', [query])
        except RangeLimited:
            if start == end:
                raise
            middle = (start + end) // 2
            return owner_logs(provider, owners, start, middle) + owner_logs(provider, owners, middle + 1, end)
        if not isinstance(batch, list) or any(row['address'].lower() != MARGIN or row.get('removed')
                or not start <= int(row['blockNumber'], 16) <= end for row in batch):
            raise ValueError('Invalid owner log range')
        rows.extend(batch)
    return _dedupe_logs(rows)


def corroborate(addresses, confirmed_from, target):
    owners = [_addr_topic(a) for a in addresses]
    indexed = list(chain.from_iterable(
        owner_logs(INDEX, owners, first, min(first + 999999, target))
        for first in range(START, target + 1, 1000000)
    ))
    # The cumulative public scanner retains the owner even after their last
    # withdrawal. Any indexed event predating a proof of absence requires a
    # second, complete scan from the protocol's first block.
    if any(int(row['blockNumber'], 16) < confirmed_from for row in indexed):
        confirmed_from = START
    def read(first):
        rows = owner_logs(OFFICIAL, owners, first, min(first + 9999, target))
        time.sleep(0.25)
        return rows
    with ThreadPoolExecutor(max_workers=4) as pool:
        independent = list(chain.from_iterable(pool.map(read, range(confirmed_from, target + 1, 10000))))
    if log_signature(independent) != log_signature(indexed):
        raise ValueError('Independent sources disagree on the wallet history')
    return _sort_logs(independent)


def older_public_checkpoint():
    # Needed only for a wallet present in the recovery seed but absent from
    # its original canonical history. Future first-time wallets use the
    # much smaller interval following the previous reconciled publication.
    path = ('https://raw.githubusercontent.com/Twojekrypto/Dolomite-dashboard/'
            'ae4b2b6686c3c5ede763796d00abc96d3ace6349/data/earn-netflow/berachain.json')
    response = requests.get(path, timeout=30, allow_redirects=False)
    response.raise_for_status()
    if hashlib.sha256(response.content).hexdigest() != '0edf1c4c1823ca31a6bf93deccf2047edfd9691dd0f803050e7dc0f366c59794':
        raise ValueError('Historical public checkpoint changed')
    doc = json.loads(response.content)
    if not doc['scanComplete'] or doc['lastBlock'] != 28971765 or len(doc['netflows']) != 41541:
        raise ValueError('Incomplete historical owner index')
    return doc


def verified_start(address, prior, seed, older=None):
    if address not in prior[1]:
        return prior[0] + 1
    if address not in seed['netflows']:
        return seed['lastBlock'] + 1
    if older is not None and address not in older['netflows']:
        return older['lastBlock'] + 1
    return START


def backfill(root, snapshot, date, public_flows, prior, seed, apply_logs, *, read=corroborate):
    history = root / 'data/earn-subaccount-history'
    present = {p.stem for p in (history / CHAIN).glob('0x*.json')}
    expected = public_flows['netflows']
    target = public_flows['lastBlock']
    if target != int(snapshot['chainMetadata'][CHAIN]['blockNumber']):
        raise ValueError('History and netflows use different checkpoints')
    missing = sorted((set(snapshot['snapshots'][CHAIN]) | expected.keys()) - present)
    if not missing:
        return
    older = None
    if any(a in seed['netflows'] for a in missing):
        try:
            older = older_public_checkpoint()
        except requests.RequestException:
            print('::warning::Historical checkpoint unavailable; comparing full onchain history.')
    generated = {}
    for offset in range(0, len(missing), 32):
        group = missing[offset: offset + 32]
        first = min(verified_start(a, prior, seed, older) for a in group)
        try:
            rows = read(group, first, target)
        except (RuntimeError, RangeLimited, ValueError):
            print(f'::warning::Could not corroborate {len(group)} new wallet histories; leaving them unverified.')
            continue
        totals = {}
        apply_logs(totals, rows)
        histories = {a: _empty_history(CHAIN, MARGIN, a, target, start_block=START,
                      source_snapshot_date=date, source_netflow_last_block=target) for a in group}
        for row in rows:
            for entry in decode_log_entries(row):
                owner = entry['owner'].lower()
                if owner in histories:
                    _append_event(histories[owner], entry, row)
        for a in group:
            observed = totals.get(a, {})
            original = expected.get(a, {})
            if not observed or set(observed) != set(original) or any(
                # Old netflow records can have only the lifetime total. Gross
                # categories were introduced later and can be partial.
                observed[m]['t'] != v['t'] or any(int(observed[m][k]) < int(n)
                       for k, n in v.items() if k != 't') or (verified_start(a, prior, seed, older) > START
                       and any(observed[m][k] != n for k,n in v.items())) for m,v in original.items()
            ):
                print(f'::warning::No reconciled historical baseline for {a}; leaving wallet unverified.')
                continue
            generated[a] = _finalize_history(histories[a])
    if generated:
        _write_histories(history, CHAIN, generated, target, start_block=START,
                         selection_address_count=len(present)+len(missing))
        print(f'Reconciled {len(generated)} previously missing Berachain histories '
              f'with {sum(v["summary"]["eventCount"] for v in generated.values())} wallet events.',flush=True)
