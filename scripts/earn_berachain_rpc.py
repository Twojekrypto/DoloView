"""Verify the archive RPC against a finalized range with independently observed Dolomite events."""
import hashlib
import json
import os
import re
import time
import threading
from urllib.parse import urlsplit

import requests

from scan_earn_netflow import ALL_EVENTS, CHAINS

connections = threading.local()
REFERENCE = '34a549f7bc4c33cf188c8cd87f5581a4c24d9483e475ca9f8521b5a34dca3af1'


def endpoint():
    value = os.environ.get('ALCHEMY_BERACHAIN_RPC', '').strip()
    if re.fullmatch(r'[A-Za-z0-9_-]{16,128}', value):
        value = 'https://berachain-mainnet.g.alchemy.com/v2/' + value
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Expected the HTTPS RPC URL or Alchemy API key in the Actions secret')
    return value


class RangeLimited(Exception):
    pass


def call(url, method, params):
    if not hasattr(connections, 'http'):
        connections.http = requests.Session()
    for attempt in range(5):
        try:
            response = connections.http.post(url, json={'jsonrpc':'2.0','id':1,'method':method,'params':params},
                                     timeout=(8, 30), allow_redirects=False)
        except requests.RequestException:
            if attempt == 4:
                raise RuntimeError('RPC connection timed out or failed') from None
        else:
            if response.status_code not in (429, 500, 502, 503, 504):
                try:
                    body = response.json()
                except ValueError:
                    raise RuntimeError(f'RPC returned HTTP {response.status_code} without JSON') from None
                error = body.get('error') or {}
                message = str(error.get('message', '')).lower()
                if method == 'eth_getLogs' and any(x in message for x in ('block range','range limit','too many results','response size')):
                    raise RangeLimited()
                if response.ok and isinstance(body, dict) and 'result' in body:
                    return body['result']
                if error.get('code') != 429:
                    raise RuntimeError(f"RPC rejected {method} (HTTP {response.status_code}, RPC {error.get('code')})")
        time.sleep(2**attempt)
    raise RuntimeError(f'RPC rate limit persisted for {method}')


def verify(url, *, compact=False):
    if int(call(url,'eth_chainId',[]),16) != 80094:
        raise ValueError('RPC is connected to the wrong chain')
    first, last = 7_003_001, 7_004_000
    expected, fingerprint = 151, REFERENCE
    if compact:
        first, last = 7_003_221, 7_003_230
        expected, fingerprint = 16, '212badf74bf10335b10cf6d107da858782ee7d2cb1ae346bf190deb6c4c562e0'
    items = []
    step = last-first+1
    while first <= last:
        end = min(first+step-1,last)
        query = {'fromBlock':hex(first),'toBlock':hex(end),'address':CHAINS['berachain']['margin'],'topics':[list(ALL_EVENTS)]}
        try:
            rows = call(url,'eth_getLogs',[query])
        except RangeLimited:
            if step == 1:
                raise ValueError('RPC does not serve historical Dolomite logs') from None
            step = 10 if step > 10 else 1
            continue
        items.extend((r['blockHash'],r['transactionHash'],int(r['logIndex'],16)) for r in rows)
        first = end + 1
    digest = hashlib.sha256(json.dumps(sorted(items)).encode()).hexdigest()
    if len(items) != expected or digest != fingerprint:
        raise ValueError(f'Historical logs differ from the finalized reference ({len(items)}/{expected} events)')
    print(f'Berachain Mainnet verified: {expected} historical Dolomite events, exact fingerprint, request span {step} blocks.', flush=True)
    return step


if __name__ == '__main__':
    verify(endpoint())
