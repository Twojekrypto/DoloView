"""Publish the public Earn dashboard files from a verified private generation."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import json
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
    dataset = path.parts[1]
    if path.suffix != '.json':
        return False
    if dataset == SHARDS:
        return path.name == 'manifest.json' or (
            len(path.parts) == 4 and len(path.stem) == 2 and all(c in '0123456789abcdef' for c in path.stem))
    if dataset == SNAPS:
        return path.name == 'manifest.json' or (len(path.stem) == 10 and path.stem >= FIRST)
    return path.as_posix() == 'data/earn-freshness/status.json'


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
    if store.read_current()[1] != source_etag:
        raise ValueError('Private generation changed while publishing')
    if result != prior:
        store.conditional_put('/web/state', store.encode(result), tag)
    if store.request('GET', '/web/state').json() != result:
        raise ValueError('Public index readback failed')
    print(f"Published {len(result['files'])} web files; uploaded {total:,} compressed bytes.")


if __name__ == '__main__':
    publish()
