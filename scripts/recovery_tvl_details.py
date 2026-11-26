"""Preserve TVL history and publish only complete borrower vote simulations."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import subprocess
import sys
import zipfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness

sys.path.insert(0, str(ROOT))
from validate_data import _veborrow_simulation_valid

BUNDLE = 'verified-tvl-details.zip'
LLAMA = 'defillama_data.json'
VOTES = 'veborrow_simulation.json'
FILES = {LLAMA, VOTES}


def days(series):
    return {entry['date'] // 86400 for entry in series}


def verify(votes, llama):
    if not _veborrow_simulation_valid(votes) or votes['status'] != 'ok' or votes['errors']:
        raise ValueError('Incomplete veBorrow simulation')
    voting = votes['totals']['onchainVoteFetch']
    count = len(votes['borrowers'])
    if (count == 0 or voting['status'] != 'ok' or voting['failedBatches']
            or voting['requestedWallets'] != count or voting['fetchedWallets'] != count
            or any(row['veDoloVoteSource'] != 'onchain_getVotes' for row in votes['borrowers'])):
        raise ValueError('Incomplete onchain vote coverage')
    stamp = datetime.fromisoformat(votes['generatedAt'].replace('Z', '+00:00'))
    if set(votes['chains']) != {'Ethereum','Arbitrum','Berachain'} or any(
        row['status'] != 'ok' or row['subgraphBlockNumber'] <= 0
        or abs(stamp.timestamp() - row['subgraphBlockTimestamp']) > 3600
        for row in votes['chains'].values()
    ):
        raise ValueError('Missing or outdated source blocks')
    for key in ('tvl', 'totalSupply'):
        series = llama[key]
        dates = [entry['date'] for entry in series]
        if len(series) < 2 or dates != sorted(set(dates)) or any(row['totalLiquidityUSD'] <= 0 for row in series):
            raise ValueError('Incomplete DeFiLlama history for ' + key)
        if max(days(series)) < int(stamp.timestamp()) // 86400 - 1:
            raise ValueError('DeFiLlama history ends before the last closed day')
    return stamp


def read(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if (len(archive.infolist()) != len(FILES) or set(archive.namelist()) != FILES
                or sum(item.file_size for item in archive.infolist()) > 60 * 1024**2):
            raise ValueError('Invalid TVL details archive')
        return {name: archive.read(name) for name in FILES}


def checked_time(raw):
    files = read(raw)
    return verify(json.loads(files[VOTES]), json.loads(files[LLAMA]))


def build(target):
    previous = previous_file(BUNDLE)
    baseline = json.loads(read(previous)[LLAMA] if previous is not None else (ROOT / LLAMA).read_bytes())
    # Borrower votes are queried onchain. Locked token amounts still use the
    # published holder inventory, which may have advanced since the checkout.
    holder_file = 'vedolo_holders.json'
    holders = [record for record in (
        (ROOT / holder_file).read_bytes(), previous_file(holder_file),
        previous_file(holder_file, 'https://twojekrypto.github.io/DoloView-legacy-feeds/'),
    ) if record is not None]
    def inventory_time(raw):
        doc = json.loads(raw)
        if not doc['holders'] or int(doc['snapshot_block']) <= 0:
            raise ValueError('Incomplete veDOLO holder inventory')
        return datetime.fromisoformat(doc['timestamp'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
    (ROOT / holder_file).write_bytes(max(holders, key=inventory_time))
    subprocess.run([sys.executable, 'validate_data.py', holder_file], cwd=ROOT, check=True)
    quote = previous_file('dolo_price.json')
    if quote is not None:
        (ROOT / 'dolo_price.json').write_bytes(quote)
    try:
        check_freshness(['dolo_price.json'], ROOT)
    except ValueError:
        subprocess.run([sys.executable, 'update_dolo_price.py'], cwd=ROOT, check=True)
        check_freshness(['dolo_price.json'], ROOT)
    for script in ('fetch_defillama.py', 'fetch_veborrow_simulation.py'):
        subprocess.run([sys.executable, script], cwd=ROOT, check=True)
    subprocess.run([sys.executable, 'validate_data.py', LLAMA, VOTES], cwd=ROOT, check=True)
    latest = json.loads((ROOT / LLAMA).read_bytes())
    if any(not days(baseline[key]).issubset(days(latest[key])) for key in ('tvl','totalSupply')):
        raise ValueError('Previously published TVL dates are missing')
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(FILES):
            archive.write(ROOT / name, name)
    print('Verified TVL history and complete onchain vote coverage:', checked_time(target.read_bytes()))


def stage(candidate, output):
    old = previous_file(BUNDLE)
    options = [data for data in (old, candidate.read_bytes() if candidate.is_file() else None) if data is not None]
    if not options:
        raise ValueError('No verified TVL details to publish')
    chosen = max(options, key=checked_time)
    for name, content in read(chosen).items():
        (output / name).write_bytes(content)
    (output / BUNDLE).write_bytes(chosen)


if __name__ == '__main__':
    if sys.argv[1] == 'build':
        build(Path(sys.argv[2]))
    elif sys.argv[1] == 'stage':
        stage(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        raise SystemExit('Expected build or stage')
