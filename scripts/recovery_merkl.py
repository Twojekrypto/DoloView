"""Publish the browser fallback using the public Ethereum ledger and Merkl API."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
import re
import sys
import time
import zipfile

import requests

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness

sys.path.insert(0, str(ROOT))
import build_earn_merkl_rewards_cache as merkl
from build_earn_verified_ledger_shards import decode_compact_ledger

PUBLIC = 'https://doloview-earn-storage.twojekrypto.workers.dev/data/earn-verified-ledger-shards/'
BUNDLE = 'verified-earn-merkl.zip'
DIRECTORY = 'data/earn-merkl-rewards'
MANIFEST = DIRECTORY + '/manifest.json'
WALLET = re.compile(DIRECTORY + r'/ethereum/(0x[0-9a-f]{40})\.json')


def read_public(path):
    for attempt in range(3):
        try:
            response = requests.get(PUBLIC + path, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2**attempt)


def read_addresses():
    meta = read_public('manifest.json')['chains']['ethereum']
    today = datetime.now(timezone.utc).date()
    if not 0 <= (today - date.fromisoformat(meta['snapshotDate'])).days <= 1:
        raise ValueError('Public Ethereum ledger is overdue')
    if not meta['shards'] or len(meta['shards']) != meta['shardCount']:
        raise ValueError('Incomplete Ethereum ledger manifest')

    def read_shard(item):
        prefix, entry = item
        if not re.fullmatch('[0-9a-f]{2}', prefix) or entry['path'] != f'ethereum/{prefix}.json':
            raise ValueError('Invalid ledger path')
        shard = read_public(entry['path'])
        ledgers = shard['ledgers']
        if (shard['chain'] != 'ethereum' or shard['prefix'] != prefix or
                len(ledgers) != entry['addressCount'] or shard['snapshotDate'] != meta['snapshotDate']):
            raise ValueError('Incomplete or mixed Ethereum ledger generation')
        eligible = []
        for address, encoded in ledgers.items():
            if not re.fullmatch('0x' + prefix + '[0-9a-f]{38}', address):
                raise ValueError('Unexpected address in public ledger')
            decoded = decode_compact_ledger(shard, encoded)
            if 'markets' not in decoded:
                raise ValueError('Unreadable Ethereum ledger record')
            if merkl.has_eligible_market('ethereum', decoded):
                eligible.append(address)
        return len(ledgers), eligible

    with ThreadPoolExecutor(max_workers=6) as pool:
        shards = list(pool.map(read_shard, meta['shards'].items()))
    if sum(count for count, _ in shards) != meta['addressCount']:
        raise ValueError('Ethereum ledger address count disagrees with manifest')
    if read_public('manifest.json')['chains']['ethereum'] != meta:
        raise ValueError('Public ledger changed while reading wallets')
    return sorted(address for _, addresses in shards for address in addresses)


def verified(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        names = {entry.filename for entry in entries}
        if (len(entries) != len(names) or MANIFEST not in names or
                sum(entry.file_size for entry in entries) > 100 * 1024**2 or
                any(name != MANIFEST and not WALLET.fullmatch(name) for name in names)):
            raise ValueError('Unexpected Merkl bundle contents')
        manifest = json.loads(archive.read(MANIFEST))
        counts = manifest['chains']['ethereum']
        if (manifest['version'] != 1 or counts['failureCount'] != 0 or manifest['failures']['ethereum'] or
                counts['eligibleAddressCount'] < 1 or counts['markets'] != merkl.ELIGIBLE_MARKETS['ethereum']):
            raise ValueError('Incomplete Merkl scan')
        stamp = manifest['generatedAt']
        current = 0
        for name in names - {MANIFEST}:
            row = json.loads(archive.read(name))
            valid_source = row['source'] == 'merkl-v4-protocol-rewards' or (
                row['source'] == 'merkl-v3-rewards' and row['generatedAt'] < stamp)
            if (row['address'] != WALLET.fullmatch(name)[1] or row['chain'] != 'ethereum' or
                    not valid_source or not row['rewards'] or row['generatedAt'] > stamp):
                raise ValueError('Invalid Merkl reward record')
            current += row['generatedAt'] == stamp
        if current != counts['rewardAddressCount'] or current > counts['eligibleAddressCount']:
            raise ValueError('Published Merkl wallet count disagrees with scan')
        return stamp


def restore(raw, root):
    verified(raw)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        archive.extractall(root)


def build(target):
    previous = previous_file(BUNDLE)
    if previous:
        with zipfile.ZipFile(io.BytesIO(previous)) as archive:
            published = set(archive.namelist())
        restore(previous, ROOT)
        # A fresh checkout contains the original seed, including wallets since removed.
        for seed in (ROOT/DIRECTORY/'ethereum').glob('0x*.json'):
            if seed.relative_to(ROOT).as_posix() not in published:
                seed.unlink()
    addresses = read_addresses()
    if not addresses:
        raise ValueError('Empty Ethereum Merkl wallet scope')
    summary = merkl.build_cache('ethereum', None, None, 0.25, 20, 3, addresses=addresses)
    if summary['chains']['ethereum']['failureCount']:
        raise ValueError('Merkl failed for some wallets')
    check_freshness([MANIFEST], ROOT)
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        for path in [ROOT/MANIFEST, *sorted((ROOT/DIRECTORY/'ethereum').glob('*.json'))]:
            archive.write(path,path.relative_to(ROOT).as_posix())
    verified(target.read_bytes())
    print(summary['chains']['ethereum'])


def stage(candidate, output):
    old = previous_file(BUNDLE)
    if candidate.is_file():
        fresh = candidate.read_bytes()
        if old is None or verified(fresh) >= verified(old):
            old = fresh
    if old is not None:
        restore(old, output)
        (output/BUNDLE).write_bytes(old)


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'build':
        build(Path(sys.argv[2]))
    elif mode == 'stage':
        stage(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        raise SystemExit('Expected build or stage')
