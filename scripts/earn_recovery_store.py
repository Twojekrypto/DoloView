"""Hydrate and publish verified Earn datasets using the private, scoped R2 transport."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import requests

from scripts.recovery_data_packs import check_manifest, pack, restore

URL = 'https://doloview-earn-storage.twojekrypto.workers.dev'
SEED_HASH = '2f9c043bc20001f9342794a5336f32af7bf6c7f33f02f3a81a9d6f40bcc51d60'
DATASETS = {'earn-snapshots', 'earn-subaccount-history', 'earn-verified-ledger',
            'earn-resolved-interest-ledger', 'earn-verified-ledger-shards',
            'earn-historical-prices', 'earn-quality', 'earn-freshness'}


def encode(record):
    return json.dumps(record, separators=(',', ':'), sort_keys=True).encode()


def request(method, path, *, optional=False, **kwargs):
    headers = {'Authorization': 'Bearer ' + os.environ['EARN_STORAGE_KEY'], **kwargs.pop('headers', {})}
    for attempt in range(4):
        try:
            response = requests.request(method, URL + path, allow_redirects=False, timeout=(15, 120),
                                        headers=headers, **kwargs)
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
            continue
        if response.status_code == 404 and optional:
            return None
        if 200 <= response.status_code < 300:
            return response
        # A timed-out PUT can already have committed. Verify the exact bytes
        # before accepting a precondition failure as an acknowledged retry.
        if attempt and method == 'PUT' and response.status_code == 412:
            saved = request('GET', path)
            if hashlib.sha256(saved.content).hexdigest() == headers['X-Content-Sha256']:
                return saved
        if response.status_code not in (408, 429, 500, 502, 503, 504, 520, 522, 524) or attempt == 3:
            raise RuntimeError(f'Earn storage {method} {path}: HTTP {response.status_code}')
        retry = response.headers.get('Retry-After', '')
        time.sleep(min(30, max(2 ** attempt, int(retry) if retry.isdecimal() else 0)))



def locator(name, item):
    if 'slot' in item:
        if type(item['slot']) is not int or item['slot'] not in (0, 1):
            raise ValueError('Invalid Earn storage slot')
        return f"/slots/{item['slot']}/" + hashlib.sha256(name.encode()).hexdigest()
    return '/base/' + filename(item)


def filename(item):
    return f"{item['sha256']}.tar.{item['codec']}"


def read_current():
    response = request('GET', '/state', optional=True)
    if response is not None:
        manifest = check_manifest(response.json())
        if {k.split('/')[0] for k in manifest['chunks']} != DATASETS:
            raise ValueError('Incomplete Earn publication')
        return manifest, response.headers['ETag']
    raw = request('GET', '/base').content
    if hashlib.sha256(raw).hexdigest() != SEED_HASH:
        raise ValueError('Unexpected recovery baseline')
    manifest = check_manifest(json.loads(raw))
    manifest['chunks'] = {k: v for k, v in manifest['chunks'].items() if k.split('/')[0] in DATASETS}
    manifest['files'] = sum(v['files'] for v in manifest['chunks'].values())
    return check_manifest(manifest), None


def hydrate(root, state):
    manifest, etag = read_current()
    with tempfile.TemporaryDirectory() as work:
        staging = Path(work)
        objects = staging / 'objects'
        objects.mkdir()

        def download(pair):
            name, item = pair
            body = request('GET', locator(name, item)).content
            if len(body) != item['bytes'] or hashlib.sha256(body).hexdigest() != item['sha256']:
                raise ValueError(f'Corrupt stored chunk: {name}')
            (objects / filename(item)).write_bytes(body)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(download, manifest['chunks'].items()))
        (staging / 'manifest.json').write_bytes(encode(manifest))
        dest = staging / 'restored'
        restore(staging, dest)
        for folder in (dest / 'data').iterdir():
            target = root / 'data' / folder.name
            if target.exists():
                if folder.name not in {'earn-quality', 'earn-historical-prices', 'earn-freshness'}:
                    raise ValueError(f'Expected an empty checkout for {folder.name}')
                # The verified remote version takes precedence over seeded checkout data.
                target.rename(staging / ('checkout-' + folder.name))
            shutil.move(str(folder), str(target))
    state.write_bytes(encode({'manifest': manifest, 'etag': etag}))
    print(f"Restored {manifest['files']:,} files from verified storage.")


def conditional_put(path, body, etag):
    digest = hashlib.sha256(body).hexdigest()
    condition = {'If-Match': etag} if etag else {'If-None-Match': '*'}
    request('PUT', path, data=body, headers={'X-Content-Sha256': digest, **condition})


def publish(root, state):
    snapshot = json.loads(state.read_bytes())
    previous = check_manifest(snapshot['manifest'])
    live, etag = read_current()
    if live != previous or etag != snapshot['etag']:
        raise ValueError('A newer generation has already published')
    with tempfile.TemporaryDirectory() as work:
        output = Path(work) / 'packed'
        manifest = pack(root, output, previous, datasets=DATASETS, workers=2)
        if set(k for k in previous['chunks']) - set(manifest['chunks']):
            raise ValueError('Refusing to remove a previously published data group')
        if {k.split('/')[0] for k in manifest['chunks']} != DATASETS:
            raise ValueError('Missing required Earn dataset')

        def upload(pair):
            name, item = pair
            old = previous['chunks'].get(name, {})
            if old.get('contentSha256') == item['contentSha256']:
                return 0
            item['slot'] = 1 - old.get('slot', 1)
            path = locator(name, item)
            head = request('HEAD', path, optional=True)
            data = (output / 'objects' / filename(item)).read_bytes()
            conditional_put(path, data, head.headers['ETag'] if head is not None else None)
            if request('GET', path).content != data:
                raise ValueError(f'Readback failed: {name}')
            return len(data)

        with ThreadPoolExecutor(max_workers=6) as pool:
            sizes = list(pool.map(upload, manifest['chunks'].items()))
        body = encode(manifest)
        if manifest != previous:
            conditional_put('/state', body, etag)
        reread, _ = read_current()
        if reread != manifest:
            raise ValueError('Published manifest failed verification')
        print(f"Verified {manifest['files']:,} files, wrote {sum(bool(s) for s in sizes)} changed chunks ({sum(sizes):,} bytes).")


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('action', choices=['hydrate', 'publish'])
    cli.add_argument('state', type=Path)
    args = cli.parse_args()
    (hydrate if args.action == 'hydrate' else publish)(Path.cwd(), args.state)
