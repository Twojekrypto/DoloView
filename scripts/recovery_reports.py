"""Preserve, verify and stage the public Supply history, pool health and rewards reports."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import zipfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness

BUNDLE = 'verified-supply-reports.zip'
HEALTH = 'data/supply-health/latest.json'
MARKETS = 'data/supply-history/manifest.json'
REWARDS = 'rewards_programs.json'
FIXED = {HEALTH, MARKETS, REWARDS, 'data/supply-health/history.json', 'data/rewards-programs-history.json'}
TOKEN = re.compile(r'data/supply-history/[a-z_]+/0x[0-9a-f]{40}\.json')
CHAINS = {'ethereum', 'arbitrum', 'berachain'}


def unpack(data, root):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        files = archive.infolist()
        names = {i.filename for i in files}
        if (len(files) != len(names) or not FIXED.issubset(names) or len(names) > 4000
                or sum(i.file_size for i in files) > 100 * 1024**2
                or any(n not in FIXED and not TOKEN.fullmatch(n) for n in names)):
            raise ValueError('Invalid public supply reports bundle')
        for item in files:
            dest = root/item.filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(archive.read(item))


def verify(root):
    read = lambda name: json.loads((root/name).read_bytes())
    markets, health, rewards = map(read, [MARKETS, HEALTH, REWARDS])
    if set(markets['refreshedChains']) != CHAINS or set(health['chains']) != CHAINS or health['errors'] or not health['markets']:
        raise ValueError('Incomplete Supply coverage')
    if rewards['errors'] or not rewards['programs'] or not all(rewards['sources'][p]['ok'] for p in ('merkl','odolo')):
        raise ValueError('Incomplete Rewards coverage')
    if rewards['inputs']['supplyHealthGeneratedAt'] != health['generatedAt'] or rewards['inputs']['supplyHistoryGeneratedAt'] != markets['generatedAt']:
        raise ValueError('Rewards do not reference the matching supply generation')
    for chain in markets['chains']:
        for skipped in chain.get('skippedTokens', []):
            if Decimal(skipped['supplyLiquidityUSD']) > Decimal(markets['activeUsdThreshold']):
                raise ValueError('Missing active supply market in '+chain['chain'])
        for token in chain['tokens']:
            path = token['path']
            if not TOKEN.fullmatch(path):
                raise ValueError('Invalid supply market path')
            points = read(path)['points']
            stamps = [int(p['timestamp']) for p in points]
            if len(stamps)<2 or stamps != sorted(set(stamps)) or any(Decimal(str(p['value']))<0 for p in points):
                raise ValueError('Invalid supply history in '+path)


def refresh(target):
    previous = previous_file(BUNDLE)
    if previous is not None:
        unpack(previous, ROOT)
    quote = previous_file('dolo_price.json')
    if quote is None:
        raise ValueError('Missing published DOLO price')
    (ROOT/'dolo_price.json').write_bytes(quote)
    check_freshness(['dolo_price.json'], ROOT)
    for command in [
        ['generate_supply_history.py','--chains','ethereum,arbitrum,berachain','--token-limit','0','--active-usd-threshold','0'],
        ['generate_supply_health.py'],
        ['fetch_rewards_programs.py'],
    ]:
        subprocess.run([sys.executable,*command],cwd=ROOT,check=True)
    verify(ROOT)
    check_freshness([MARKETS,HEALTH,REWARDS], ROOT)
    paths = FIXED | {p.relative_to(ROOT).as_posix() for p in (ROOT/'data/supply-history').glob('*/*.json')}
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        for name in sorted(paths):
            archive.write(ROOT/name,name)
    print(f'{len(paths)} verified public files, {target.stat().st_size:,} compressed bytes')


def stage(output, candidate):
    old = previous_file(BUNDLE)
    if old is None and not candidate.is_file():
        print('No completed reports publication yet')
        return
    data = candidate.read_bytes() if candidate.is_file() else old
    def date(raw):
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            return datetime.fromisoformat(json.loads(archive.read(HEALTH))['generatedAt'].replace('Z','+00:00'))
    if old is not None and date(old) > date(data):
        data = old
    unpack(data, output)
    verify(output)
    (output/BUNDLE).write_bytes(data)
    if (datetime.now(timezone.utc)-date(data)).total_seconds() > 8*3600:
        print('::warning::Supply and rewards reports exceed eight hours; original timestamps retained')


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('mode', choices=['build','stage'])
    cli.add_argument('bundle',type=Path)
    cli.add_argument('--output',type=Path)
    args = cli.parse_args()
    if args.mode == 'build':
        refresh(args.bundle)
    else:
        stage(args.output,args.bundle)
