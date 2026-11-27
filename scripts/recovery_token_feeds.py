"""Collect and stage verified public DOLO, veDOLO and oDOLO feeds."""

import argparse
from decimal import Decimal
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qs, urlsplit
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'scripts')]
from check_dashboard_freshness import _path_value, assess, load_config, parse_timestamp
from recovery_market_feeds import previous_file
from recovery_vedolo_flows import validate as validate_vedolo_flows
from rpc_client import RpcClient, RpcError, get_endpoints
from validate_data import RULES, ValidationResult, validate_file


FAMILIES = {
    'dolo-holders': ('dolo_holders.json',),
    'dolo-flows': ('dolo_flows.json',),
    'vedolo-inventory': ('vedolo_holders.json', 'vedolo_stats.json', 'vedolo_expiry.json',
                         'data/vedolo-vote-power-history.json', 'vedolo_early_unlock.json'),
    'odolo-flow': ('odolo_flows.json',),
    'odolo-activity': ('exercised_usd.json', 'avg_lock_data.json', 'exercisers_by_address.json'),
    'odolo-contract': ('odolo_contract_data.json',),
    'early-exits': ('early_exits.json',),
}
CLOUDFLARE_LEGACY = 'https://doloview.pages.dev/legacy-assets/'
GITHUB_LEGACY = 'https://twojekrypto.github.io/DoloView-legacy-feeds/'
MAX_AGE_MINUTES = 240
FLOW_INPUT = 'vedolo_flows.json'
CLAIM_INPUTS = ('data/odolo-claim-events.json', 'data/reward-claim-events.json',
                'data/reward-claim-events/berachain.json')
FRESHNESS = {
    rule['path']: dict(rule, maxAgeMinutes=MAX_AGE_MINUTES, sourceMaxAgeMinutes=MAX_AGE_MINUTES,
                       earlyDueMinutes=120, sourceEarlyDueMinutes=120)
    for rule in load_config(ROOT / 'config/dashboard_freshness.json')['artifacts']
    if rule['path'] in {path for files in FAMILIES.values() for path in files}
}
PROGRESS_FIELDS = {
    'dolo_flows.json': ('cex_history_meta.blocks.eth', 'cex_history_meta.blocks.bera'),
    'vedolo_holders.json': ('snapshot_block', 'stats.total_minted', 'stats.total_burned',
                            'stats.nft_transfer_count', 'stats.nft_transfer_max_block'),
    'vedolo_early_unlock.json': ('sourceBlock',),
    'data/vedolo-vote-power-history.json': ('targetBlock',),
    'odolo_flows.json': ('current_block', 'transfer_coverage.transfer_count',
                        'claim_source_reconciliation.coverage_to_block'),
    'odolo_contract_data.json': ('blockNumber',),
    'early_exits.json': ('coverage.toBlock', 'coverage.eventCount'),
    'exercised_usd.json': ('last_block', 'total_txs', 'total_usdc', 'activityCoverage.toBlock'),
    'avg_lock_data.json': ('total_exercises', 'valid_durations'),
    'exercisers_by_address.json': ('total_exercises', 'total_odolo_exercised_exercises'),
}


def get_previous(family):
    files = {}
    for name in FAMILIES[family]:
        raw = previous_file(name)
        if raw is None:
            legacy = GITHUB_LEGACY if name == 'dolo_flows.json' else CLOUDFLARE_LEGACY
            raw = previous_file(name, base=legacy)
        files[name] = raw if raw is not None else (ROOT / name).read_bytes()
    return files


def write_files(files, root):
    for name, raw in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)


def unpack(family, raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        expected = FAMILIES[family]
        if (len(entries) != len(expected) or {entry.filename for entry in entries} != set(expected)
                or any(not 0 < entry.file_size <= FRESHNESS[entry.filename]['maxBytes']
                       for entry in entries)):
            raise ValueError('Unexpected token feed bundle members or size')
        return {entry.filename: archive.read(entry) for entry in entries}


def validate_progress(name, current, previous):
    fields = PROGRESS_FIELDS.get(name, ())
    for field in fields:
        value = _path_value(current, field)
        floor = _path_value(previous, field)
        if type(value) not in (int, float) or value < 0 or (floor is not None and value < floor):
            raise ValueError(f'{name} {field} regressed')
    timestamp_key = FRESHNESS[name]['timestamps'][0]
    if parse_timestamp(_path_value(current, timestamp_key)) < parse_timestamp(_path_value(previous, timestamp_key)):
        raise ValueError(f'{name} publication timestamp regressed')


def block_timestamp(chain, block):
    if type(block) is not int or block <= 0:
        raise ValueError(f'{chain} source block is invalid')
    raw = RpcClient(chain=chain, timeout=12).call('eth_getBlockByNumber', [hex(block), False])
    if not isinstance(raw, dict) or int(raw.get('number', '0x0'), 16) != block:
        raise ValueError(f'{chain} source block metadata is unavailable')
    return int(raw['timestamp'], 16)


def require_fresh_source(chain, block, now, timestamp=block_timestamp):
    age = now - timestamp(chain, block)
    if not 0 <= age < MAX_AGE_MINUTES * 60:
        raise ValueError(f'{chain} source block {block} is outside the four-hour publication window')


def validate_activity(documents):
    usd = documents['exercised_usd.json']
    duration = documents['avg_lock_data.json']
    exercises = documents['exercisers_by_address.json']
    # The established USD counter rounds each incremental update to cents.
    if (usd['total_txs'] != exercises['total_odolo_exercised_exercises']
            or abs(usd['total_usdc'] - exercises['total_odolo_exercise_usdc_paid']) > 1
            or duration['total_exercises'] != exercises['total_exercises']
            or duration['valid_durations'] != sum(duration['distribution'].values())
            or not 0 < duration['valid_durations'] <= duration['total_exercises']):
        raise ValueError('oDOLO activity reports do not reconcile')


def vester_logs_at(client, start, end):
    from generate_vedolo_flows import ODOLO_VESTER, TRANSFER_TOPIC

    params = {'address': ODOLO_VESTER, 'topics': [TRANSFER_TOPIC],
              'fromBlock': hex(start), 'toBlock': hex(end)}
    logs = client.call('eth_getLogs', [params])
    if not isinstance(logs, list) or any(
        not start <= int(log['blockNumber'], 16) <= end
        or log['address'].lower() != ODOLO_VESTER
        or log['topics'][0].lower() != TRANSFER_TOPIC
        or len(log['topics']) != 4 or log['data'] != '0x'
        or log.get('removed') is True or len(log['transactionHash']) != 66
        for log in logs
    ):
        raise ValueError('Invalid Vester transfer log result')
    return [dict(log, data='0x0') for log in logs]


def verified_vester_logs(clients, start, end):
    from generate_dolo_flows import rpc_provider_family, select_transfer_log_quorum, TransferLogQuorumError

    responses = []
    for endpoint, client in clients.items():
        try:
            # The public dRPC Berachain endpoint limits every getLogs call to
            # 100 blocks. It still provides an independent vote for the full range.
            width = 100 if rpc_provider_family(endpoint) == 'drpc.org' else end - start + 1
            logs = []
            for block in range(start, end + 1, width):
                logs.extend(vester_logs_at(client, block, min(block + width - 1, end)))
            responses.append((endpoint, logs))
            return select_transfer_log_quorum(responses)
        except (IndexError, KeyError, TypeError, ValueError, RpcError, TransferLogQuorumError):
            continue
    raise TransferLogQuorumError(f'Vester transfer history has no RPC quorum for {start:,}-{end:,}')


def collect_activity_rows(start, target, client, anchor_block, anchor_hash):
    from generate_dolo_flows import rpc_provider_family
    from generate_vedolo_flows import FINALITY_REORG_DEPTH

    if not 0 < start <= target or target - start > 1_000_000:
        raise ValueError('oDOLO activity checkpoint is outside a bounded recovery range')
    header = client.call('eth_getBlockByNumber', [hex(target), False])
    # A healthy current head is necessary but insufficient. Some RPC log
    # providers report successful empty archival results on known events.
    clients = {}
    for endpoint in get_endpoints('berachain'):
        peer = RpcClient(endpoints=[endpoint], timeout=9, retries_per_endpoint=2, quiet=True)
        try:
            peer_header = peer.call('eth_getBlockByNumber', [hex(target), False])
            anchor_logs = vester_logs_at(peer, anchor_block, anchor_block)
        except (KeyError, TypeError, ValueError, RpcError):
            continue
        if (peer_header and peer_header['hash'] == header['hash']
                and any(log['transactionHash'].lower() == anchor_hash for log in anchor_logs)):
            clients[endpoint] = peer
    if len({rpc_provider_family(endpoint) for endpoint in clients}) < 2:
        raise ValueError('Two independent Berachain providers are required for activity')

    txs = {}
    families = set()
    for block in range(start, target + 1, 10_000):
        logs, proof = verified_vester_logs(clients, block, min(block + 9_999, target))
        for log in logs:
            tx_hash = log['transactionHash'].lower()
            block_number = int(log['blockNumber'], 16)
            if tx_hash in txs and txs[tx_hash] != (block_number, log['blockHash']):
                raise ValueError('Vester logs disagree on transaction block')
            txs[tx_hash] = (block_number, log['blockHash'])
        families.update(proof['providerFamilies'])
    return txs, {'fromBlock': start, 'toBlock': target, 'sourceTimestamp': int(header['timestamp'], 16),
                 'providerFamilies': sorted(families), 'confirmationBlocks': FINALITY_REORG_DEPTH}


def read_new_exercise(client, tx_hash, block, block_hash):
    from generate_exercisers import RECEIPT_CACHE_VERSION, parse_exercise_receipt, round_amount
    from generate_vedolo_flows import ODOLO_VESTER
    from odolo_exercises import EXERCISE_METHOD_DOLO, EXERCISE_METHOD_IDS, extract_lock_duration_days, extract_lock_duration_seconds

    raw = client.call('eth_getTransactionByHash', [tx_hash])
    if (not raw or raw['hash'].lower() != tx_hash or raw['blockHash'] != block_hash
            or int(raw['blockNumber'], 16) != block):
        raise ValueError('Vester transaction could not be verified')
    method = raw['input'][:10].lower()
    if (raw['to'] or '').lower() != ODOLO_VESTER or method not in EXERCISE_METHOD_IDS:
        return None
    receipt = client.call('eth_getTransactionReceipt', [tx_hash])
    header = client.call('eth_getBlockByNumber', [hex(block), False])
    if (not receipt or receipt['status'] != '0x1' or receipt['blockHash'] != block_hash
            or receipt['transactionHash'].lower() != tx_hash or not header or header['hash'] != block_hash):
        raise ValueError('Vester receipt could not be verified')
    tx = {'hash': tx_hash, 'input': raw['input'], 'from': raw['from'],
          'timeStamp': int(header['timestamp'], 16), 'methodId': method}
    usdc, odolo, dolo = parse_exercise_receipt(receipt)
    seconds = extract_lock_duration_seconds(tx)
    paired = method == EXERCISE_METHOD_DOLO
    if not seconds or not odolo or (dolo is None if paired else usdc is None):
        raise ValueError('Vester exercise has unsupported payment or lock data')
    details = {'usdc': round_amount(usdc), 'odolo': round_amount(odolo),
               'dolo_paid': round_amount(dolo), 'paid_token': 'DOLO' if paired else 'USDC.e',
               'lock_seconds': seconds, 'lock_days': extract_lock_duration_days(tx),
               'receipt_version': RECEIPT_CACHE_VERSION}
    return tx, details, Decimal(str(usdc)) if not paired else Decimal(0)


def refresh_odolo_activity():
    from calculate_avg_lock import lock_statistics
    from generate_exercisers import (build_exerciser_snapshot, load_vedolo_holder_lookup,
                                     load_vedolo_lock_token_lookup, seed_cache_from_existing_output)
    from generate_vedolo_flows import FINALITY_REORG_DEPTH

    usd_path = ROOT / 'exercised_usd.json'
    exercise_path = ROOT / 'exercisers_by_address.json'
    usd = json.loads(usd_path.read_bytes())
    previous = json.loads(exercise_path.read_bytes())
    # The already-public, validated history supplies exact lock durations and
    # historical receipt amounts. Only new transactions need live receipts.
    cache = seed_cache_from_existing_output()
    transactions = []
    old_token_ids = {}
    for wallet in previous['exercisers']:
        for row in wallet['txs']:
            transactions.append({'hash': row['hash'], 'from': wallet['address'], 'timeStamp': row['timestamp']})
            if row.get('token_ids'):
                old_token_ids[row['hash']] = row['token_ids']
    if (len(cache) != len(transactions) or len(cache) != previous['total_exercises']
            or any(not entry['lock_seconds'] for entry in cache.values())):
        raise ValueError('Published exercise history is incomplete')

    client = RpcClient(chain='berachain', timeout=12, quiet=True)
    latest = max(transactions, key=lambda tx: tx['timeStamp'])
    anchor_hash = latest['hash'].lower()
    evidence = client.call('eth_getTransactionByHash', [anchor_hash])
    if not evidence or evidence['hash'].lower() != anchor_hash:
        raise ValueError('Latest published exercise could not be anchored')
    anchor_block = int(evidence['blockNumber'], 16)
    checkpoint = usd.get('activityCoverage')
    if checkpoint and checkpoint == previous.get('activityCoverage'):
        baseline = checkpoint['toBlock']
    else:
        baseline = min(usd['last_block'], anchor_block)
    target = int(client.call('eth_blockNumber', []), 16) - FINALITY_REORG_DEPTH
    if target < baseline:
        raise ValueError('Finalized Vester scan target regresses the prior checkpoint')
    candidates, coverage = collect_activity_rows(baseline, target, client, anchor_block, anchor_hash)
    new_usdc = Decimal(0)
    for tx_hash, (block, block_hash) in candidates.items():
        if tx_hash in cache:
            continue
        exercise = read_new_exercise(client, tx_hash, block, block_hash)
        if exercise is None:
            continue
        tx, cache[tx_hash], amount = exercise
        new_usdc += amount
        if cache[tx_hash]['paid_token'] == 'USDC.e':
            usd['total_txs'] += 1
        transactions.append(tx)
    transactions.sort(key=lambda tx: (tx['timeStamp'], tx['hash']))
    holders, holder_timestamp = load_vedolo_holder_lookup()
    token_lookup = {**old_token_ids, **load_vedolo_lock_token_lookup()}
    exercises = build_exerciser_snapshot(transactions, cache, holders, holder_timestamp, token_lookup)
    average = lock_statistics([entry['lock_seconds'] for entry in cache.values()], len(transactions))
    usd.update(total_usdc=float(round(Decimal(str(usd['total_usdc'])) + new_usdc, 2)),
               last_block=target, last_updated=exercises['updated'])
    for name, payload in zip(FAMILIES['odolo-activity'], (usd, average, exercises)):
        payload['activityCoverage'] = coverage
        (ROOT / name).write_text(json.dumps(payload, indent=2) + '\n')
    print(f'Verified Vester activity through block {target:,}: {len(transactions):,} exercise transactions')


def validate(family, root, previous, now):
    result = ValidationResult()
    current = {}
    for name in FAMILIES[family]:
        path = root / name
        if not 0 < path.stat().st_size <= FRESHNESS[name]['maxBytes']:
            raise ValueError(f'{name} exceeds its publication size limit')
        if path.name in RULES:
            validate_file(path, RULES[path.name], result)
        current[name] = document = json.loads(path.read_bytes())
        validate_progress(name, document, json.loads(previous[name]))
        freshness = assess(document, FRESHNESS[name], now)
        if freshness['state'] not in ('fresh', 'early_due'):
            raise ValueError(f'{name} freshness gate failed ({freshness["state"]})')
    if result.failed or result.skipped:
        raise ValueError(f'{family} data validation failed')
    if family == 'odolo-activity':
        validate_activity(current)
        coverage = current['exercised_usd.json']['activityCoverage']
        if (any(doc['activityCoverage'] != coverage for doc in current.values())
                or coverage['toBlock'] != current['exercised_usd.json']['last_block']
                or not 0 < coverage['fromBlock'] <= coverage['toBlock']
                or len(set(coverage['providerFamilies'])) < 2
                or not 0 <= now - coverage['sourceTimestamp'] < MAX_AGE_MINUTES * 60):
            raise ValueError('oDOLO activity has incomplete or stale RPC coverage')
    elif family == 'vedolo-inventory':
        stats = current['vedolo_stats.json']['stats']
        holders = current['vedolo_holders.json']
        vote = current['data/vedolo-vote-power-history.json']
        if (stats != holders['stats'] or stats.get('_stale_data')
                or stats['unique_holders'] != len(holders['holders'])
                or stats['active_nfts'] != sum(row['nft_count'] for row in holders['holders'])
                or stats['total_vote_weight_block'] != vote['targetBlock']):
            raise ValueError('veDOLO inventory and canonical vote power do not reconcile')
    return current


def redact(line):
    for name, value in os.environ.items():
        if not value or not ('RPC' in name or name.endswith('_API_KEY')):
            continue
        parsed = urlsplit(value) if value.startswith(('https://', 'http://')) else None
        secrets = [value]
        if parsed:
            secrets.extend(parsed.path.split('/'))
            secrets.extend(part for parts in parse_qs(parsed.query).values() for part in parts)
            secrets.extend([parsed.username, parsed.password])
        for secret in sorted((item for item in secrets if item and len(item) >= 8), key=len, reverse=True):
            line = line.replace(secret, '[redacted]')
    return line


def run(*command, timeout=3600):
    # The legacy generators can include provider errors in their logs.
    with tempfile.TemporaryFile(mode='w+t', encoding='utf-8', errors='replace') as log:
        try:
            subprocess.run([sys.executable, *command], cwd=ROOT, check=True, timeout=timeout,
                           stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.seek(0)
            for line in log:
                print(redact(line), end='')


def refresh_input(name):
    raw = previous_file(name)
    if raw is not None:
        (ROOT / name).write_bytes(raw)


def indexed_ownership_at(block):
    from generate_vedolo_flows import DEPLOY_BLOCK, address_from_topic, fetch_indexed_transfer_logs

    for row in fetch_indexed_transfer_logs(DEPLOY_BLOCK, block):
        topics = row['topics']
        if len(topics) != 4:
            raise ValueError('Malformed veDOLO ownership Transfer event')
        yield {'blockNumber': int(row['blockNumber'], 16),
               'transactionIndex': int(row['logIndex'], 16),
               'tokenID': int(topics[3], 16),
               'from': address_from_topic(topics[1]), 'to': address_from_topic(topics[2])}


def refresh_vedolo_inventory():
    """Feed the existing snapshot generator from its verified on-chain block."""
    import update_data

    source = validate_vedolo_flows((ROOT / FLOW_INPUT).read_bytes())
    block = source['target_block']
    require_fresh_source('berachain', block, int(time.time()))
    update_data.VEDOLO_SNAPSHOT_BLOCK = block
    if not update_data.API_KEY:
        update_data.fetch_all_nft_transfers = lambda: list(indexed_ownership_at(block))
    update_data.main()


def refresh_dolo_flows():
    import generate_dolo_flows

    if not generate_dolo_flows.has_complete_verified_baseline(generate_dolo_flows.load_state()):
        sys.argv.append('--full-verified-backfill')
    generate_dolo_flows.main()


def collect(family):
    if family == 'dolo-holders':
        for name in ('dolo_flows.json', 'vesting_investors.json'):
            refresh_input(name)
        run('generate_dolo_holders.py', timeout=6000)
    elif family == 'dolo-flows':
        for name in ('dolo_holders.json', 'vedolo_holders.json', FLOW_INPUT, 'data/dolo-liquidity.json'):
            refresh_input(name)
        run('scripts/recovery_token_feeds.py', 'dolo-backfill', family, timeout=9900)
        run('validate_data.py', 'dolo_holder_wallet_history.json', timeout=120)
    elif family == 'vedolo-inventory':
        refresh_input(FLOW_INPUT)
        run('scripts/recovery_token_feeds.py', 'inventory', family, timeout=4400)
        run('validate_vedolo_locked_history.py', '--flows', FLOW_INPUT, '--holders', 'vedolo_holders.json', timeout=180)
        run('generate_vedolo_vote_power_history.py', '--sync-stats', timeout=1800)
        run('generate_vedolo_early_unlock.py', timeout=1500)
    elif family == 'odolo-flow':
        for name in (*CLAIM_INPUTS, 'exercisers_by_address.json', FLOW_INPUT):
            refresh_input(name)
        run('generate_reward_claim_events.py', timeout=2400)
        run('generate_odolo_flows.py', timeout=4200)
    elif family == 'odolo-activity':
        for name in ('vedolo_holders.json', FLOW_INPUT):
            refresh_input(name)
        run('scripts/recovery_token_feeds.py', 'activity', family, timeout=2800)
    elif family == 'odolo-contract':
        run('fetch_odolo_contract.py', timeout=240)
    elif family == 'early-exits':
        run('fetch_early_exits.py', timeout=4200)


def check_source_blocks(family, files, now):
    if family == 'dolo-holders':
        state = json.loads((ROOT / 'dolo_holders_state.json').read_bytes())
        for key, chain in (('eth', 'ethereum'), ('bera', 'berachain')):
            if state[f'skipped_ranges_{key}']:
                raise ValueError(f'{chain} DOLO holders scan has unresolved transfer ranges')
            require_fresh_source(chain, state[f'{key}_last_block'], now)
    elif family == 'dolo-flows':
        for key, chain in (('eth', 'ethereum'), ('bera', 'berachain')):
            require_fresh_source(chain, files['dolo_flows.json']['cex_history_meta']['blocks'][key], now)
    elif family == 'vedolo-inventory':
        require_fresh_source('berachain', files['vedolo_holders.json']['snapshot_block'], now)
    elif family == 'odolo-flow':
        require_fresh_source('berachain', files['odolo_flows.json']['current_block'], now)
    elif family == 'odolo-contract':
        require_fresh_source('berachain', files['odolo_contract_data.json']['blockNumber'], now)
    elif family == 'early-exits':
        require_fresh_source('berachain', files['early_exits.json']['coverage']['toBlock'], now)


def build(family, bundle):
    previous = get_previous(family)
    write_files(previous, ROOT)
    collect(family)
    now = int(time.time())
    current = validate(family, ROOT, previous, now)
    if any((ROOT / name).read_bytes() == old for name, old in previous.items()):
        raise ValueError(f'{family} generator left a previously published file unchanged')
    check_source_blocks(family, current, now)
    with zipfile.ZipFile(bundle, 'x', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in FAMILIES[family]:
            archive.write(ROOT / name, name)
    print(f'Validated {family}: {bundle.stat().st_size:,} bytes')


def stage(family, bundle, output):
    files = previous = get_previous(family)
    if bundle.is_file():
        try:
            candidate = unpack(family, bundle.read_bytes())
            with tempfile.TemporaryDirectory(prefix='token-feed-') as directory:
                root = Path(directory)
                write_files(candidate, root)
                validate(family, root, previous, int(time.time()))
            files = candidate
        except (ValueError, TypeError, KeyError, RuntimeError, zipfile.BadZipFile) as exc:
            print(f'::warning::Retaining prior {family} publication ({type(exc).__name__})')
    write_files(files, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('build', 'stage', 'inventory', 'dolo-backfill', 'activity'))
    parser.add_argument('family', choices=FAMILIES)
    parser.add_argument('bundle', type=Path, nargs='?')
    parser.add_argument('--output', type=Path, default=Path('_data'))
    args = parser.parse_args()
    if args.mode == 'inventory':
        refresh_vedolo_inventory()
    elif args.mode == 'activity':
        refresh_odolo_activity()
    elif args.mode == 'dolo-backfill':
        refresh_dolo_flows()
    elif args.bundle is None:
        parser.error('a bundle path is required')
    elif args.mode == 'build':
        build(args.family, args.bundle)
    else:
        stage(args.family, args.bundle, args.output)


if __name__ == '__main__':
    main()
