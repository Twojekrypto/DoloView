"""Publish the public Earn dashboard files from a verified private generation."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import json
import re
import time

import requests
from pathlib import Path
import tempfile

from scripts import earn_recovery_store as store
from scripts.restore_recovery_seed import unpack

SHARDS = 'earn-verified-ledger-shards'
SNAPS = 'earn-snapshots'
FIRST = '2026-11-25'
CHAINS = {'ethereum', 'arbitrum', 'berachain'}


def groups_to_read(name):
    dataset, _, suffix = name.partition('/')
    return dataset in {SHARDS, 'earn-freshness'} or (
        dataset == SNAPS and (suffix == 'index' or suffix >= FIRST[:7]))


def eligible(path):
    value = path.as_posix()
    if value in {'data/earn-freshness/status.json', f'data/{SHARDS}/manifest.json', f'data/{SNAPS}/manifest.json'}:
        return True
    if re.fullmatch(r'data/earn-verified-ledger-shards/(?:arbitrum|berachain|ethereum|mantle|xlayer)/[0-9a-f]{2}\.json', value):
        return True
    day = re.fullmatch(r'data/earn-snapshots/(\d{4}-\d{2}-\d{2})\.json', value)
    return bool(day and day[1] >= FIRST)


def bundle(paths, root):
    body = bytearray()
    entries = {}
    for path in sorted(paths):
        value = (root / path).read_bytes()
        json.loads(value)
        compressed = gzip.compress(value, compresslevel=6, mtime=0)
        entries['/' + path.as_posix()] = [len(body), len(compressed)]
        body.extend(compressed)
    return bytes(body), entries


def validate_generation(today, read):
    index = read(SNAPS+'/manifest.json')
    daily = read(f'{SNAPS}/{today}.json')
    ledger = read(SHARDS+'/manifest.json')
    freshness = read('earn-freshness/status.json')
    if not CHAINS.issubset(index['chains'].get(today, [])) or daily['date'] != today:
        raise ValueError('Public Earn snapshot date is out of sync')
    for chain in sorted(CHAINS):
        meta = ledger['chains'][chain]
        if (meta['snapshotDate'] != today or not daily['snapshots'][chain]
                or int(daily['chainMetadata'][chain]['blockNumber']) <= 0 or chain not in freshness['chains']):
            raise ValueError('Missing public Earn data for ' + chain)
        prefix, shard = min(meta['shards'].items())
        if shard['path'] != f'{chain}/{prefix}.json' or not re.fullmatch('[a-f0-9]{2}', prefix):
            raise ValueError('Unexpected public wallet shard path')
        sample = read(SHARDS+'/'+shard['path'])
        if (sample['chain'] != chain or sample['prefix'] != prefix or sample['snapshotDate'] != today
                or not sample['ledgers'] or len(sample['ledgers']) != shard['addressCount']):
            raise ValueError('Public wallet shard disagrees with its index for ' + chain)


def check_web_response(today):
    def read(path):
        url = store.URL + '/data/' + path
        for attempt in range(3):
            try:
                response = requests.get(url, params={'verify': time.time_ns()}, timeout=(10, 45),
                                        allow_redirects=False)
                response.raise_for_status()
                if response.headers.get('Access-Control-Allow-Origin') != '*':
                    raise ValueError('Public dashboard CORS missing for ' + path)
                return response.json()
            except (requests.RequestException, ValueError):
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
    validate_generation(today, read)
    print('Anonymous readback verified the daily snapshot, active-chain wallet shards and freshness report.')


def publish():
    source, source_etag = store.read_current()
    if not source_etag:
        raise ValueError('The verified Earn workflow has not yet published')
    response = store.request('GET', '/web/state', optional=True)
    tag = response.headers['ETag'] if response is not None else None
    prior = response.json() if response is not None else {'files': {}, 'groups': {}}
    today = datetime.now(timezone.utc).date().isoformat()
    def prepare(pair):
        name, item = pair
        group = hashlib.sha256(name.encode()).hexdigest()
        previous = prior['groups'].get(group)
        if previous and previous['source'] == item['contentSha256']:
            return group, previous, {p: e for p, e in prior['files'].items() if e[0] == group}, 0
        with tempfile.TemporaryDirectory() as work:
            root = Path(work)
            raw = store.request('GET', store.locator(name, item)).content
            if hashlib.sha256(raw).hexdigest() != item['sha256']:
                raise ValueError('Private source checksum failed for ' + name)
            archive = root / store.filename(item)
            archive.write_bytes(raw)
            unpack(archive, root, item['files'])
            paths = [p.relative_to(root) for p in (root/'data').rglob('*.json')]
            paths = [p for p in paths if eligible(p)]
            if not paths:
                return group, None, {}, 0
            body, entries = bundle(paths, root)
            if name == SNAPS + '/index':
                snap = json.loads((root/'data'/SNAPS/'manifest.json').read_bytes())
                if not CHAINS.issubset(snap['chains'].get(today, [])):
                    raise ValueError('Missing current snapshot for an active chain')
        slot = 1 - previous['slot'] if previous else 0
        path = f'/web/slots/{slot}/{group}'
        head = store.request('HEAD', path, optional=True)
        store.conditional_put(path, body, head.headers['ETag'] if head is not None else None)
        copy = store.request('GET', path)
        if copy.content != body:
            raise ValueError('Public projection readback failed for ' + name)
        meta = {'slot': slot, 'etag': copy.headers['ETag'].strip('"'), 'source': item['contentSha256']}
        return group, meta, {p:[group,*lengths] for p,lengths in entries.items()}, len(body)

    result = {'source': source_etag, 'files': {}, 'groups': {}}
    candidates = [(name, item) for name, item in source['chunks'].items() if groups_to_read(name)]
    total = 0
    with ThreadPoolExecutor(max_workers=5) as pool:
        for group, meta, entries, size in pool.map(prepare, candidates):
            if meta:
                result['groups'][group] = meta
                result['files'].update(entries)
                total += size
    required = {f'/data/{SNAPS}/{today}.json',f'/data/{SNAPS}/manifest.json',
                f'/data/{SHARDS}/manifest.json','/data/earn-freshness/status.json'}
    if not required.issubset(result['files']):
        raise ValueError('Public data are incomplete or out of date')
    if not set(prior['files']).issubset(result['files']):
        raise ValueError('Previously published files would disappear')
    def read_candidate(path):
        group, offset, length = result['files']['/data/' + path]
        meta = result['groups'][group]
        response = store.request('GET', f"/web/slots/{meta['slot']}/{group}")
        if response.headers['ETag'].strip('"') != meta['etag']:
            raise ValueError('Staged Earn data changed during verification')
        return json.loads(gzip.decompress(response.content[offset:offset + length]))

    if result != prior:
        validate_generation(today, read_candidate)
    if store.read_current()[1] != source_etag:
        raise ValueError('Private generation changed while publishing')
    if result != prior:
        store.conditional_put('/web/state', store.encode(result), tag)
    if store.request('GET', '/web/state').json() != result:
        raise ValueError('Public index readback failed')
    check_web_response(today)
    print(f"Published {len(result['files'])} web files; uploaded {total:,} compressed bytes.")


if __name__ == '__main__':
    publish()
