"""Advance the existing public Berachain netflows through an explicitly pinned block."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT))
from recovery_market_feeds import previous_file
from scan_earn_netflow import add_flow, decode_log_entries, flow_category
from scripts.earn_berachain_rpc import call, endpoint
from scripts.refresh_earn_berachain import ArchiveReader

FILE = Path('data/earn-netflow/berachain.json')
SEED = '47016edb6cf6b0ab47d275511a07ea3a0b1118ebf5b7f5bff69c0569a0aba44b'


def checked(raw):
    doc = json.loads(raw)
    flows = doc['netflows']
    if (doc['chain'] != 'berachain' or doc['scanComplete'] is not True or doc['scanStatus'] != 'complete'
            or doc['addressCount'] != len(flows) or not flows or int(doc['lastBlock']) < 29243078):
        raise ValueError('Incomplete public netflow baseline')
    for markets in flows.values():
        for entry in markets.values():
            if 't' not in entry or set(entry) - set('tdwsxlv'):
                raise ValueError('Unexpected netflow schema or cycle-aware baseline')
            for key, value in entry.items():
                if not isinstance(value, str) or str(int(value)) != value or (key != 't' and int(value) < 0):
                    raise ValueError('Invalid netflow amount')
    return doc


def baseline(candidate=None, limit=None, allow_seed_offline=False):
    seed = (ROOT / FILE).read_bytes()
    if hashlib.sha256(seed).hexdigest() != SEED:
        raise ValueError('The public recovery baseline changed')
    try:
        published = previous_file(FILE.as_posix())
    except (requests.RequestException, RuntimeError):
        if not allow_seed_offline:
            raise
        print('Published netflows unavailable; using the pinned public recovery baseline.', flush=True)
        published = None
    candidates = [seed, published]
    if candidate is not None and candidate.is_file():
        candidates.append(candidate.read_bytes())
    versions = {}
    for raw in candidates:
        if raw is None:
            continue
        doc = checked(raw)
        block = doc['lastBlock']
        if limit is not None and block > limit:
            continue
        if block in versions and versions[block]['netflows'] != doc['netflows']:
            raise ValueError(f'Candidate totals disagree at block {block}')
        versions[block] = doc
    if not versions:
        raise ValueError('No verified public baseline before the requested block')
    blocks = sorted(versions)
    for older, newer in zip(blocks, blocks[1:]):
        prior, current = versions[older]['netflows'], versions[newer]['netflows']
        if any(not set(markets).issubset(current.get(address, {})) for address, markets in prior.items()):
            raise ValueError('A previously published wallet or market is missing')
    return versions[blocks[-1]]


def apply_logs(flows, logs):
    seen = set()
    for row in logs:
        key = (row['blockHash'], row['logIndex'])
        if key in seen:
            raise ValueError('Duplicate log in finalized onchain interval')
        seen.add(key)
        category = flow_category(row['topics'][0].lower())
        entries = decode_log_entries(row)
        if not entries:
            raise ValueError('Could not decode a balance-changing event')
        for entry in entries:
            add_flow(flows, entry['owner'].lower(), entry['market'], int(entry['delta']), category)


def advance(doc, reader, target):
    start = doc['lastBlock'] + 1
    if not start - 1 <= target <= reader.head:
        raise ValueError('Invalid netflow target')
    def pinned(number):
        params = [hex(number), False]
        block = call(reader.url, 'eth_getBlockByNumber', params)
        independent = call('https://rpc.berachain.com/', 'eth_getBlockByNumber', params)
        if not block['hash'] or block['hash'] != independent['hash'] or int(block['number'], 16) != number:
            raise ValueError('Netflow providers disagree on the pinned block')
        return block
    if doc.get('lastBlockHash') and doc['lastBlockHash'] != pinned(start-1)['hash']:
        raise ValueError('The previous netflow checkpoint no longer matches the chain')
    block = pinned(target)
    count = 0
    for first in range(start, target + 1, 1000):
        rows = reader.logs(first, min(first + 999, target))
        apply_logs(doc['netflows'], rows)
        count += len(rows)
    if pinned(target)['hash'] != block['hash']:
        raise ValueError('The pinned block changed during replay')
    updated = datetime.fromtimestamp(int(block['timestamp'],16), timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    doc.update(lastBlock=target, lastBlockHash=block['hash'], updatedAt=updated, addressCount=len(doc['netflows']))
    print(f'Netflows through {target:,}: {count} logs since {start-1:,}; {doc["addressCount"]:,} wallets', flush=True)


def save(doc, destination):
    destination.parent.mkdir(exist_ok=True, parents=True)
    raw = json.dumps(doc, separators=(',', ':')).encode()
    checked(raw)
    tmp = destination.with_suffix('.tmp')
    tmp.write_bytes(raw)
    tmp.replace(destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['refresh','restore','stage'])
    parser.add_argument('--artifact', type=Path)
    parser.add_argument('--output', type=Path, default=ROOT)
    parser.add_argument('--block', type=int)
    args = parser.parse_args()
    target = args.block
    if args.action in ('refresh', 'restore'):
        if target is None:
            snapshots = ROOT / 'data/earn-snapshots'
            date = max(json.loads((snapshots/'manifest.json').read_bytes())['dates'])
            target = int(json.loads((snapshots/(date+'.json')).read_bytes())['chainMetadata']['berachain']['blockNumber'])
    if args.action == 'refresh':
        doc = baseline(limit=target, allow_seed_offline=True)
        with ThreadPoolExecutor(max_workers=2) as pool:
            advance(doc, ArchiveReader(endpoint(), pool), target)
    else:
        doc = baseline(args.artifact, limit=target, allow_seed_offline=args.action == 'restore')
    save(doc, args.output / FILE)
