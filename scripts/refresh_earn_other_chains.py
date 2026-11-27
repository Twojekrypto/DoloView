"""Advance existing private Ethereum and Arbitrum Earn histories from matching RPC logs."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import tempfile

from apply_earn_subaccount_history_delta import apply_delta_histories
from scan_earn_netflow import ALL_EVENTS, CHAINS, decode_log_entries
from scan_earn_subaccount_history_events import scan_chain_to_event_shards
from scripts.earn_berachain_rpc import RangeLimited, call
from scripts.refresh_earn_berachain import log_signature


SOURCES = {
    'ethereum': (1, ('https://mainnet.gateway.tenderly.co', 'https://rpc.mevblocker.io'), 5_000, 50_000),
    'arbitrum': (42161, ('https://arb1.arbitrum.io/rpc', 'https://arbitrum.gateway.tenderly.co'), 10_000, 1_200_000),
}
ADDRESS = re.compile(r'0x[0-9a-f]{40}')
HASH = re.compile(r'0x[0-9a-f]{64}')


class QuorumReader:
    def __init__(self, chain, pool):
        self.chain = chain
        self.pool = pool
        chain_id, self.sources, self.chunk_size, _ = SOURCES[chain]
        self.margin = CHAINS[chain]['margin'].lower()
        heads = []
        for source in self.sources:
            if int(call(source, 'eth_chainId', []), 16) != chain_id:
                raise ValueError(f'{chain}: archive source is on the wrong chain')
            heads.append(int(call(source, 'eth_blockNumber', []), 16))
        self.head = min(heads)

    def anchor(self, number):
        if not 0 < number <= self.head:
            raise ValueError(f'{self.chain}: archives have not reached block {number:,}')

        def header(source):
            block = call(source, 'eth_getBlockByNumber', [hex(number), False])
            if not isinstance(block, dict) or int(block['number'], 16) != number:
                raise ValueError(f'{self.chain}: missing pinned block {number:,}')
            block_hash = block['hash'].lower()
            timestamp = int(block['timestamp'], 16)
            if not HASH.fullmatch(block_hash) or timestamp <= 0:
                raise ValueError(f'{self.chain}: invalid pinned block {number:,}')
            return block_hash, timestamp

        one, two = self.pool.map(header, self.sources)
        if one != two:
            raise ValueError(f'{self.chain}: archives disagree at pinned block {number:,}')
        return one

    def _read(self, source, first, last):
        query = {'fromBlock': hex(first), 'toBlock': hex(last), 'address': self.margin,
                 'topics': [ALL_EVENTS]}
        logs = call(source, 'eth_getLogs', [query])
        if not isinstance(logs, list) or any(
                row['address'].lower() != self.margin or row.get('removed') is not False
                or not first <= int(row['blockNumber'], 16) <= last
                or not HASH.fullmatch(row['blockHash'].lower())
                or not HASH.fullmatch(row['transactionHash'].lower())
                or not row['topics'] or row['topics'][0].lower() not in ALL_EVENTS
                for row in logs):
            raise ValueError(f'{self.chain}: invalid archive logs for {first:,}–{last:,}')
        signature = log_signature(logs)
        if len({(row[3], row[4]) for row in signature}) != len(logs):
            raise ValueError(f'{self.chain}: duplicate archive log identities for {first:,}–{last:,}')
        return logs, signature

    def _verified_range(self, first, last):
        try:
            (rows, one), (_, two) = self.pool.map(lambda source: self._read(source, first, last), self.sources)
        except RangeLimited:
            if first == last:
                raise ValueError(f'{self.chain}: an archive cannot serve block {first:,}') from None
            middle = (first + last) // 2
            return self._verified_range(first, middle) + self._verified_range(middle + 1, last)
        if one != two:
            raise ValueError(f'{self.chain}: archives disagree on logs for {first:,}–{last:,}')
        for row in rows:
            evidence = decode_log_entries(row)
            if not evidence or any(not ADDRESS.fullmatch(entry['owner']) for entry in evidence):
                raise ValueError(f'{self.chain}: undecodable archive event for {first:,}–{last:,}')
        return rows

    def logs(self, first, last):
        events = []
        for start in range(first, last + 1, self.chunk_size):
            events.extend(self._verified_range(start, min(start + self.chunk_size - 1, last)))
        return events


def read_blocks(directory, chain):
    blocks = {}
    for path in sorted(directory.glob('0x*.json')):
        wallet = json.loads(path.read_bytes())
        block = int(wallet['lastScannedBlock'])
        span = wallet['scanRange']
        if (not ADDRESS.fullmatch(path.stem) or wallet['address'].lower() != path.stem
                or wallet['chain'] != chain or not 0 < int(span['fromBlock']) <= block
                or int(span['toBlock']) != block):
            raise ValueError(f'{chain}: invalid private wallet baseline')
        blocks[path.stem] = block
    return blocks


def refresh(root, chain, *, max_blocks=None):
    if max_blocks is None:
        max_blocks = SOURCES[chain][3]
    if max_blocks < 1:
        raise ValueError('The maximum catch-up range must be positive')
    history = root / 'data/earn-subaccount-history'
    snapshots = root / 'data/earn-snapshots'
    snapshot_manifest = json.loads((snapshots / 'manifest.json').read_bytes())
    day = max(name for name in snapshot_manifest['dates'] if chain in snapshot_manifest['chains'][name])
    snapshot = json.loads((snapshots / (date.fromisoformat(day).isoformat() + '.json')).read_bytes())
    metadata = snapshot['chainMetadata'][chain]
    holdings = snapshot['snapshots'][chain]
    target = int(metadata['blockNumber'])
    if not (snapshot['date'] == day and metadata.get('interestIndexes') and isinstance(holdings, dict) and holdings):
        raise ValueError(f'{chain}: incomplete pinned snapshot')
    manifest_path = history / 'manifest.json'
    previous = json.loads(manifest_path.read_bytes())['chains'][chain]
    blocks = read_blocks(history / chain, chain)
    if not blocks or len(blocks) != int(previous['addressCount']) or not 0 < int(previous['lastBlock']) <= target:
        raise ValueError(f'{chain}: incomplete wallet baseline or older snapshot')
    if max(blocks.values()) > target:
        raise ValueError(f'{chain}: snapshot precedes a private wallet checkpoint')

    first = min(blocks.values()) + 1
    cutoff = min(target, first + max_blocks - 1)
    selected = sorted(address for address, block in blocks.items() if block < cutoff)
    appended = 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        reader = QuorumReader(chain, pool)
        anchor = reader.anchor(target)
        if metadata.get('blockHash') and metadata['blockHash'].lower() != anchor[0]:
            raise ValueError(f'{chain}: archives disagree with the snapshot block hash')
        cutoff_anchor = anchor if cutoff == target else reader.anchor(cutoff)
        if selected:
            with tempfile.TemporaryDirectory(dir=history.parent) as work:
                events = Path(work) / 'events'
                scanned = scan_chain_to_event_shards(chain, selected_addresses=selected, output_dir=events,
                        from_block=first, to_block=cutoff, chunk_size=reader.chunk_size, no_resume=True,
                        progress_key=None, archive_reader=reader)
                if scanned['status'] != 'completed' or scanned['lastBlockExclusive'] != cutoff + 1:
                    raise ValueError(f'{chain}: verified archive scan did not complete')
                staged = Path(work) / 'updated'
                staged.mkdir()
                (staged / 'manifest.json').write_bytes(manifest_path.read_bytes())
                for offset in range(0, len(selected), 512):
                    batch = selected[offset:offset + 512]
                    result = apply_delta_histories(chain, events_dir=events, history_dir=history,
                            output_dir=staged, addresses=batch, progress_key=None, start_index=None, end_index=None)
                    if result['updatedAddressCount'] != len(batch) or result['targetBlock'] != cutoff:
                        raise ValueError(f'{chain}: wallet replay was incomplete')
                    appended += result['eventCount']
                if read_blocks(staged / chain, chain) != dict.fromkeys(selected, cutoff):
                    raise ValueError(f'{chain}: staged histories do not match the verified block')
                if reader.anchor(target) != anchor or (cutoff != target and reader.anchor(cutoff) != cutoff_anchor):
                    raise ValueError(f'{chain}: pinned block changed during replay')
                blocks.update(dict.fromkeys(selected, cutoff))
                manifest = json.loads((staged / 'manifest.json').read_bytes())
                manifest['chains'][chain].update(lastBlock=max(blocks.values()), fromBlock=previous['fromBlock'],
                    addressCount=len(blocks), selectionAddressCount=max(len(blocks), int(previous['selectionAddressCount'])),
                    lastBatchAddressCount=len(selected))
                verified_manifest = staged / 'verified-manifest.json'
                verified_manifest.write_text(json.dumps(manifest, separators=(',', ':')))
                for path in (staged / chain).glob('*.json'):
                    path.replace(history / chain / path.name)
                verified_manifest.replace(manifest_path)

    outstanding = sum(block < target for block in blocks.values())
    unbootstrapped = len({address.lower() for address in holdings} - set(blocks))
    pinned = datetime.fromtimestamp(anchor[1], timezone.utc).isoformat(timespec='seconds')
    print(f'{chain}: verified {len(selected):,} existing histories through {cutoff:,}; {appended:,} events replayed. '
          f'{outstanding:,} existing histories remain behind snapshot {target:,} ({pinned}); '
          f'{unbootstrapped:,} active snapshot wallets lack a baseline and require a full backfill.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--chain', action='append', choices=SOURCES)
    parser.add_argument('--max-blocks', type=int, help='Maximum contiguous catch-up blocks per selected chain')
    args = parser.parse_args()
    for chain in args.chain or SOURCES:
        refresh(Path.cwd(), chain, max_blocks=args.max_blocks)


if __name__ == '__main__':
    main()
