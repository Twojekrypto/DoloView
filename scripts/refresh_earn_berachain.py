"""Advance every existing Berachain history through a pinned daily snapshot."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import time

from apply_earn_subaccount_history_delta import apply_delta_histories
from scan_earn_netflow import ALL_EVENTS, CHAINS
from scan_earn_subaccount_history_events import scan_chain_to_event_shards
from scripts.earn_berachain_rpc import call, endpoint, verify


class ArchiveReader:
    def __init__(self, url, pool):
        self.url = url
        self.pool = pool
        verify(url, compact=True)
        self.head = int(call(url, 'eth_blockNumber', []), 16)

    def read(self, url, first, last):
        margin = CHAINS['berachain']['margin'].lower()
        query = {'fromBlock': hex(first), 'toBlock': hex(last), 'address': margin,
                 'topics': [list(ALL_EVENTS)]}
        logs = call(url, 'eth_getLogs', [query])
        if not isinstance(logs, list) or any(row['address'].lower() != margin or row.get('removed')
                or not first <= int(row['blockNumber'], 16) <= last for row in logs):
            raise ValueError(f'Invalid historical logs for blocks {first}-{last}')
        return logs

    def logs(self, first, last):
        events = []
        for start in range(first, last+1, 1000):
            end = min(start+999,last)
            sources = ('https://rpc.berachain.com/', 'https://berachain.gateway.tenderly.co')
            one, two = self.pool.map(lambda source: self.read(source, start, end), sources)
            if log_signature(one) != log_signature(two):
                raise ValueError(f'Public history providers disagree for blocks {start}-{end}')
            # The private RPC permits 10 blocks per query. Check every nonempty
            # window and one control window, including fully quiet 1,000-block spans.
            windows = {start: []}
            for row in one:
                window = start + (int(row['blockNumber'],16)-start)//10*10
                windows.setdefault(window,[]).append(row)
            for block, expected in sorted(windows.items()):
                actual = self.read(self.url, block, min(end,block+9))
                if log_signature(actual) != log_signature(expected):
                    raise ValueError(f'Authenticated history disagrees near block {block}')
                time.sleep(0.35)
            events.extend(one)
        return events


def log_signature(rows):
    return sorted((int(row['blockNumber'],16), row['blockHash'].lower(), int(row['transactionIndex'],16),
                   row['transactionHash'].lower(), int(row['logIndex'],16), tuple(t.lower() for t in row['topics']),
                   row['data'].lower()) for row in rows)


def cutoffs(chain_dir):
    blocks = {}
    for path in sorted(chain_dir.glob('0x*.json')):
        row = json.loads(path.read_bytes())
        block = int(row['lastScannedBlock'])
        if block <= 0 or row['address'].lower() != path.stem or row['chain'] != 'berachain':
            raise ValueError('Invalid baseline history: ' + path.name)
        blocks[path.stem] = block
    return blocks


def refresh(root):
    chain = 'berachain'
    history = root / 'data/earn-subaccount-history'
    snapshots = root / 'data/earn-snapshots'
    date = max(json.loads((snapshots/'manifest.json').read_bytes())['dates'])
    target = int(json.loads((snapshots/(date+'.json')).read_bytes())['chainMetadata'][chain]['blockNumber'])
    before = json.loads((history/'manifest.json').read_bytes())['chains'][chain]
    blocks = cutoffs(history/chain)
    if not blocks or len(blocks) != int(before['addressCount']):
        raise ValueError('Incomplete wallet baseline')
    addresses = sorted(a for a, block in blocks.items() if block < target)
    if not addresses:
        print(f'All {len(blocks):,} existing histories already cover snapshot {target:,}.')
        return
    first = min(blocks[a] for a in addresses) + 1
    if target < int(before['lastBlock']):
        raise ValueError('Snapshot precedes the published chain checkpoint')
    with tempfile.TemporaryDirectory(dir=history.parent) as tmp, ThreadPoolExecutor(max_workers=4) as pool:
        reader = ArchiveReader(endpoint(), pool)
        if target > reader.head:
            raise ValueError('RPC has not reached the snapshot block')
        block = call(reader.url, 'eth_getBlockByNumber', [hex(target), False])
        if int(block['number'], 16) != target or not block['hash']:
            raise ValueError('Missing snapshot block')
        public_block = call('https://rpc.berachain.com/', 'eth_getBlockByNumber', [hex(target), False])
        if public_block['hash'] != block['hash']:
            raise ValueError('Providers disagree on the pinned block')
        events = Path(tmp)/'events'
        progress = scan_chain_to_event_shards(chain, selected_addresses=addresses, output_dir=events,
            from_block=first, to_block=target, chunk_size=1000, no_resume=True,
            progress_key=None, archive_reader=reader)
        if progress['status'] != 'completed' or progress['lastBlockExclusive'] != target+1:
            raise ValueError('Historical scan incomplete')
        appended = 0
        # Apply to a disposable staging directory. A failed batch never publishes a partial checkpoint.
        out = Path(tmp)/'updated'
        out.mkdir()
        (out/'manifest.json').write_bytes((history/'manifest.json').read_bytes())
        for start in range(0, len(addresses), 512):
            batch = addresses[start:start+512]
            result = apply_delta_histories(chain, events_dir=events, history_dir=history,
                output_dir=out, addresses=batch, progress_key=None, start_index=None, end_index=None)
            if result['updatedAddressCount'] != len(batch) or result['targetBlock'] != target:
                raise ValueError('Incomplete wallet replay')
            appended += result['eventCount']
        if cutoffs(out/chain) != dict.fromkeys(addresses, target):
            raise ValueError('Restored history does not cover every selected wallet')
        if call(reader.url, 'eth_getBlockByNumber', [hex(target), False])['hash'] != block['hash']:
            raise ValueError('Pinned block changed during replay')
        manifest = json.loads((out/'manifest.json').read_bytes())
        meta = manifest['chains'][chain]
        meta.update(addressCount=len(blocks), selectionAddressCount=max(len(blocks),before['selectionAddressCount']),
                    lastBatchAddressCount=len(addresses), fromBlock=before['fromBlock'])
        for path in (out/chain).glob('*.json'):
            path.replace(history/chain/path.name)
        (history/'manifest.json').write_text(json.dumps(manifest,separators=(',',':')))
        print(f'Verified {len(addresses):,} wallet histories from block {first:,} to {target:,}; '
              f'{appended:,} wallet events replayed. Snapshot {date}.',flush=True)


if __name__ == '__main__':
    refresh(Path.cwd())
