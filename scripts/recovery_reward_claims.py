"""Move the existing public reward-claim index through artifact publication."""
import argparse
import io
import json
from pathlib import Path
import subprocess
import zipfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness

NAME = 'verified-reward-claims.zip'
MANIFEST = 'data/reward-claim-events.json'
ODOLO = 'data/odolo-claim-events.json'
ACTIVE = {'arbitrum', 'berachain'}
CHAINS = ACTIVE | {'mantle', 'xlayer'}
FILES = (MANIFEST, ODOLO, *(f'data/reward-claim-events/{chain}.json' for chain in sorted(CHAINS)))


def put(files, root):
    for name, raw in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)


def unpack(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        if (len(entries) != len(FILES) or {entry.filename for entry in entries} != set(FILES)
                or sum(entry.file_size for entry in entries) > 125 * 1024**2):
            raise ValueError('Unexpected files or size in public reward claims bundle')
        return {entry.filename: archive.read(entry) for entry in entries}


def identities(rows):
    result = {(row['txHash'], row['logIndex']): (row['amountWei'], row['blockNumber'], row['user']) for row in rows}
    if len(result) != len(rows):
        raise ValueError('Duplicate reward claim event')
    return result


def validate(files, previous=None):
    manifest = json.loads(files[MANIFEST])
    meta = manifest['chains']
    if set(meta) != CHAINS or not manifest['eventsShardedByChain'] or manifest['events']:
        raise ValueError('Incomplete reward claim manifest')
    if sum(m['eventCount'] for m in meta.values()) != manifest['eventCount']:
        raise ValueError('Claim manifest totals do not reconcile')
    prior = json.loads(previous[MANIFEST]) if previous is not None else None
    for chain in CHAINS:
        name = f'data/reward-claim-events/{chain}.json'
        shard = json.loads(files[name])
        coverage = meta[chain]
        if (manifest['chainEventFiles'][chain] != name or shard['generatedAt'] != manifest['generatedAt']
                or shard['chains'] != {chain: coverage} or len(shard['events']) != coverage['eventCount']
                or any(row['chainKey'] != chain for row in shard['events'])):
            raise ValueError(f'Invalid {chain} reward claim shard')
        if chain in ACTIVE and (coverage['coverageStatus'] != 'complete' or coverage['warning']):
            raise ValueError(f'Unverified {chain} reward claim coverage')
        current = identities(shard['events'])
        if prior is not None:
            before = prior['chains'][chain]
            if int(coverage['toBlock']) < int(before['toBlock']):
                raise ValueError(f'Reward claim coverage regressed for {chain}')
            old = identities(json.loads(previous[name])['events'])
            if any(current.get(key) != value for key, value in old.items()):
                raise ValueError(f'Previously published claim changed in {chain}')
            if chain not in ACTIVE and (coverage != before or current != old):
                raise ValueError(f'Unexpected change for inactive network {chain}')
    legacy = json.loads(files[ODOLO])
    bera = json.loads(files['data/reward-claim-events/berachain.json'])
    canonical = identities(bera['events'])
    if not legacy['events'] or any(canonical.get(key) != value for key, value in identities(legacy['events']).items()):
        raise ValueError('The oDOLO compatibility index does not match Berachain')
    return manifest


def run(mode, candidate, output):
    prior = previous_file(NAME)
    if mode == 'prepare':
        if prior is not None:
            files = unpack(prior)
            validate(files)
            put(files, ROOT)
        return
    if mode == 'build':
        files = {name: (ROOT / name).read_bytes() for name in FILES}
        baseline = unpack(prior) if prior is not None else {
            name: subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)
            for name in FILES
        }
        validate(files, baseline)
        check_freshness([MANIFEST], ROOT)
        with zipfile.ZipFile(candidate, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for name, raw in files.items():
                archive.writestr(name, raw)
        return
    chosen = prior
    if candidate.is_file():
        raw = candidate.read_bytes()
        files = unpack(raw)
        candidate_meta = validate(files)
        if prior is None or candidate_meta['generatedAt'] >= validate(unpack(prior))['generatedAt']:
            validate(files, unpack(prior) if prior is not None else None)
            chosen = raw
    if chosen is not None:
        put(unpack(chosen), output)
        (output / NAME).write_bytes(chosen)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'build', 'stage'])
    parser.add_argument('--candidate', type=Path, default=Path(NAME))
    parser.add_argument('--output', type=Path, default=Path('_data'))
    args = parser.parse_args()
    run(args.mode, args.candidate, args.output)
