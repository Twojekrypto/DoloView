#!/usr/bin/env python3
"""Build and stage the public cross-market deposits and withdrawals feed."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generate_supply_activity import GRAPH_ENDPOINTS, post_json
from generate_supply_activity_overview import ADDRESS, CHAINS, TX_HASH, WINDOW_SECONDS, event_row, parse_time


PUBLIC_PATH = Path('data/supply-activity/overview.json')
MAX_AGE = 4 * 3600
MAX_BYTES = 20 * 1024**2
PAGE_SIZE = 500
PAGE_LIMIT = 600
ENTITY = {'deposit': 'deposits', 'withdraw': 'withdrawals'}
FIELDS = ('id serialId token { id } effectiveUser { id } marginAccount { id } '
          'amountDeltaWei amountUSDDeltaWei transaction { id timestamp blockNumber }')
FORMAT = {
    'schemaVersion': 1, 'scope': '30d', 'source': 'static-subgraph-activity',
    'eventGrain': 'chain-source-event', 'amountBasis': 'source-event-amount',
    'usdBasis': 'source-event-historical-quote', 'status': 'complete',
}
LEGACY_MANIFEST = 'data/supply-activity/manifest.json'
DECIMAL_INTEGER = re.compile(r'[0-9]+')


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace('+00:00', 'Z')


def positive_int(value):
    return type(value) is int and value > 0


def valid_block(block):
    return (isinstance(block, dict) and set(block) == {'hash', 'number', 'timestamp', 'deployment'}
            and positive_int(block['number']) and positive_int(block['timestamp'])
            and isinstance(block['hash'], str) and TX_HASH.fullmatch(block['hash'])
            and isinstance(block['deployment'], str) and bool(block['deployment']))


def source_block(chain, expected=None):
    pin = f'(block: {{ hash: "{expected["hash"]}" }})' if expected else ''
    source = post_json(GRAPH_ENDPOINTS[chain], {'query': f'''{{
      _meta{pin} {{ deployment hasIndexingErrors block {{ number timestamp hash }} }}
    }}'''})['_meta']
    if not isinstance(source, dict) or source.get('hasIndexingErrors') is not False:
        raise ValueError(f'{chain}: missing indexed source or reported indexing errors')
    block = {**source['block'], 'deployment': source['deployment']}
    if not valid_block(block) or (expected is not None and block != expected):
        raise ValueError(f'{chain}: indexed source changed or returned an invalid block')
    return block


def pages(chain, block, entity, where, fields):
    cursor = ''
    for _ in range(PAGE_LIMIT):
        after = f', id_gt: {json.dumps(cursor)}' if cursor else ''
        query = f'''{{
          {entity}(first: {PAGE_SIZE}, block: {{ hash: "{block['hash']}" }},
              orderBy: id, orderDirection: asc, where: {{ {where}{after} }}) {{ {fields} }}
        }}'''
        chunk = post_json(GRAPH_ENDPOINTS[chain], {'query': query}).get(entity)
        if not isinstance(chunk, list) or len(chunk) > PAGE_SIZE:
            raise ValueError(f'{chain}: invalid {entity} page')
        for row in chunk:
            identity = row['id']
            if not isinstance(identity, str) or identity <= cursor:
                raise ValueError(f'{chain}: repeated or unordered {entity} cursor')
            cursor = identity
            yield row
        if len(chunk) < PAGE_SIZE:
            return
    raise ValueError(f'{chain}: incomplete {entity} pagination')


def chain_projection(chain, block, start, end, generated):
    market_rows = list(pages(chain, block, 'tokens', 'marketId_not: null', 'id symbol name marketId'))
    tokens = [{'chain': chain, 'tokenId': raw['id'], 'symbol': raw['symbol'] or raw['id'],
               'name': raw['name'], 'marketId': raw['marketId']} for raw in market_rows]
    token_index = {token['tokenId']: index for index, token in enumerate(tokens)}
    rows = []
    where = f'transaction_: {{ timestamp_gte: "{start}", timestamp_lte: "{end}" }}'
    for kind, entity in ENTITY.items():
        for raw in pages(chain, block, entity, where, FIELDS):
            tx = raw['transaction']
            token_id = raw['token']['id']
            if not start <= int(tx['timestamp']) <= end or not 0 < int(tx['blockNumber']) <= block['number']:
                raise ValueError(f'{chain}: {entity} observation lies outside the pinned source window')
            if token_id not in token_index:
                raise ValueError(f'{chain}: {entity} returned an unlisted market {token_id}')
            evidence = {'sourceId': f"{kind}-{raw['serialId']}", 'type': kind, 'ambiguous': False,
                        'timestamp': int(tx['timestamp']), 'txHash': tx['id'], 'amount': raw['amountDeltaWei'],
                        'usd': raw['amountUSDDeltaWei'], 'wallet': raw['effectiveUser']['id'],
                        'account': raw['marginAccount']['id']}
            rows.append(event_row(evidence, token_index[token_id], chain=chain))
    source_block(chain, expected=block)
    evidence = {'chain': chain, 'status': 'complete', 'expectedAssets': len(tokens),
                'includedAssets': len(tokens), 'sourceGeneratedAt': generated, 'rowCount': len(rows),
                'skippedTokens': [], 'issues': [], 'sourceBlock': block}
    return evidence, tokens, rows


def sort_key(row, token_chain):
    return (-row[2], CHAINS.index(token_chain[row[8]]), row[0], row[1])


def coverage(assets):
    return {'status': 'complete', 'expectedAssets': assets, 'includedAssets': assets, 'unavailableChains': []}


def build_overview(*, now=None):
    with ThreadPoolExecutor(max_workers=len(CHAINS)) as pool:
        blocks = list(pool.map(source_block, CHAINS))
        now = now or datetime.now(timezone.utc)
        checked = int(now.timestamp())
        end = min(block['timestamp'] for block in blocks)
        if not 0 <= checked - end < MAX_AGE or any(block['timestamp'] > checked + 300 for block in blocks):
            raise ValueError('Source blocks do not cover a current 30-day window')
        start = end - WINDOW_SECONDS
        projections = [pool.submit(chain_projection, chain, block, start, end, stamp(end))
                       for chain, block in zip(CHAINS, blocks)]
        result = {**FORMAT, 'sourceManifest': None, 'generatedAt': stamp(end),
                  'sourceGeneratedAt': stamp(end), 'periodStart': start, 'periodEnd': end,
                  'maxSourceAgeSeconds': MAX_AGE, 'chains': [], 'tokens': [], 'rows': []}
        for future in projections:
            evidence, tokens, rows = future.result()
            for row in rows:
                row[8] += len(result['tokens'])
            result['chains'].append(evidence)
            result['tokens'].extend(tokens)
            result['rows'].extend(rows)
    token_chain = [token['chain'] for token in result['tokens']]
    result['rows'].sort(key=lambda row: sort_key(row, token_chain))
    result['coverage'] = coverage(len(result['tokens']))
    verify(result, now=now)
    return result


def verify(payload, *, now=None, require_fresh=True, allow_legacy=False):
    top_keys = set(FORMAT) | {'sourceManifest', 'generatedAt', 'sourceGeneratedAt', 'periodStart',
                            'periodEnd', 'maxSourceAgeSeconds', 'chains', 'tokens', 'rows', 'coverage'}
    if (not isinstance(payload, dict) or set(payload) != top_keys
            or any(payload[key] != value for key, value in FORMAT.items())):
        raise ValueError('Unexpected public supply activity envelope')
    manifest = payload['sourceManifest']
    legacy = allow_legacy and manifest == LEGACY_MANIFEST
    if ((manifest is not None and not legacy) or payload['maxSourceAgeSeconds'] != (86400 if legacy else MAX_AGE)):
        raise ValueError('Unexpected public supply activity source')
    now = now or datetime.now(timezone.utc)
    source_time = parse_time(payload['sourceGeneratedAt'])
    generated = parse_time(payload['generatedAt'])
    start, end = payload['periodStart'], payload['periodEnd']
    if not (positive_int(end) and positive_int(start) and end - start == WINDOW_SECONDS
            and int(source_time.timestamp()) == end and source_time == generated <= now):
        raise ValueError('Invalid 30-day public supply activity cutoff')
    if require_fresh and (now - source_time).total_seconds() >= MAX_AGE:
        raise ValueError('Public supply activity source is at least four hours old')

    tokens, rows, chains = payload['tokens'], payload['rows'], payload['chains']
    if (not all(isinstance(items, list) for items in (tokens, rows, chains))
            or payload['coverage'] != coverage(len(tokens))):
        raise ValueError('Incomplete public supply activity asset coverage')
    seen_tokens, seen_markets, token_chain = set(), set(), []
    for token in tokens:
        if not isinstance(token, dict) or set(token) != {'chain','tokenId','marketId','name','symbol'}:
            raise ValueError('Unexpected market token')
        chain, token_id, market = token['chain'], token['tokenId'], token['marketId']
        if (chain not in CHAINS or not isinstance(token_id, str) or not ADDRESS.fullmatch(token_id)
                or not isinstance(market, str) or not DECIMAL_INTEGER.fullmatch(market)
                or not isinstance(token['symbol'], str) or not token['symbol']
                or (token['name'] is not None and not isinstance(token['name'], str))
                or (chain, token_id.lower()) in seen_tokens or (chain, market) in seen_markets):
            raise ValueError('Invalid or duplicate market token')
        seen_tokens.add((chain, token_id.lower()))
        seen_markets.add((chain, market))
        token_chain.append(chain)

    seen_events, event_counts, previous = set(), Counter(), None
    for row in rows:
        if not isinstance(row, list) or len(row) != 10:
            raise ValueError('Unexpected public supply activity row')
        source_id, kind, when, tx, amount, usd, wallet, other, index, metadata = row
        if (kind not in ENTITY or not isinstance(source_id, str) or not source_id.startswith(kind + '-')
                or not DECIMAL_INTEGER.fullmatch(source_id.removeprefix(kind + '-'))
                or type(index) is not int or not 0 <= index < len(tokens)
                or not positive_int(when) or not start <= when <= end or other != ''
                or not isinstance(metadata, dict)):
            raise ValueError('Invalid public supply activity row')
        chain = token_chain[index]
        expected = event_row({'sourceId': source_id, 'type': kind, 'timestamp': when, 'txHash': tx,
                              'amount': amount, 'usd': usd if usd is not None else metadata.get('sourceUsd'),
                              'wallet': wallet, 'account': metadata.get('accountNumber'), 'ambiguous': False},
                             index, chain=chain)
        ordering = sort_key(row, token_chain)
        identity = (chain, source_id)
        if row != expected or identity in seen_events or (previous is not None and ordering < previous):
            raise ValueError('Conflicting, duplicate or unordered public activity evidence')
        previous = ordering
        seen_events.add(identity)
        event_counts[chain] += 1

    if len(chains) != len(CHAINS):
        raise ValueError('Missing public supply activity chains')
    asset_counts = Counter(token_chain)
    block_times = []
    for chain, entry in zip(CHAINS, chains):
        count = asset_counts[chain]
        expected = {'chain': chain, 'status': 'complete', 'expectedAssets': count, 'includedAssets': count,
                    'sourceGeneratedAt': payload['sourceGeneratedAt'], 'rowCount': event_counts[chain],
                    'skippedTokens': [], 'issues': []}
        if not legacy:
            block = entry.get('sourceBlock')
            if not valid_block(block) or not end <= block['timestamp'] <= int(now.timestamp()) + 300:
                raise ValueError(f'{chain}: invalid pinned source block')
            expected['sourceBlock'] = block
            block_times.append(block['timestamp'])
        if not count or entry != expected:
            raise ValueError(f'{chain}: incomplete source coverage')
    if not legacy and end != min(block_times):
        raise ValueError('Cutoff differs from indexed source coverage')
    return end


def read(raw, **validation):
    if len(raw) > MAX_BYTES:
        raise ValueError('Public supply activity file exceeds its size budget')
    return verify(json.loads(raw), **validation)


def write(target, content):
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.json.tmp')
    temporary.write_bytes(content)
    temporary.replace(target)


def previous_public_activity():
    from scripts.recovery_market_feeds import previous_file

    return previous_file(PUBLIC_PATH.as_posix())


def stage(candidate, output, *, now=None):
    candidates = []
    previous = previous_public_activity()
    if previous is not None:
        try:
            candidates.append((read(previous, now=now, require_fresh=False, allow_legacy=True), previous))
        except (ValueError, KeyError, TypeError) as exc:
            print(f'::warning::Invalid previous public supply activity: {exc}')
    if candidate.is_file():
        content = candidate.read_bytes()
        try:
            candidates.append((read(content, now=now), content))
        except (ValueError, KeyError, TypeError) as exc:
            print(f'::warning::Invalid public supply activity artifact: {exc}')
    if candidates:
        end, content = max(candidates, key=lambda item: item[0])
        write(output / PUBLIC_PATH, content)
        print(f'Public supply activity staged through {stamp(end)}')
    else:
        print('::warning::No structurally verified public supply activity available')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('build', 'verify', 'stage'))
    parser.add_argument('candidate', type=Path)
    parser.add_argument('--output', type=Path, default=Path('_data'))
    args = parser.parse_args()
    if args.command == 'stage':
        stage(args.candidate, args.output)
    elif args.command == 'verify':
        print(f'Verified public supply activity through {stamp(read(args.candidate.read_bytes()))}')
    else:
        payload = build_overview()
        content = (json.dumps(payload, separators=(',', ':'), ensure_ascii=False) + '\n').encode()
        read(content)
        write(args.candidate, content)
        print(f"Verified {len(payload['rows'])} public events across {len(payload['tokens'])} markets through {payload['sourceGeneratedAt']}")


if __name__ == '__main__':
    main()
