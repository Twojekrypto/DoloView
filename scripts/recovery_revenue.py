"""Publish revenue, signed rebates and their onchain evidence as a single generation."""
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
import subprocess
import sys
import zipfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT

sys.path.insert(0,str(ROOT))
from audit_dolomite_revenue_onchain import default_target_date
from fetch_dolomite_revenue import usable_onchain_revenue_row

BUNDLE = 'verified-revenue.zip'
REVENUE = 'dolomite_revenue.json'
REBATES = 'veborrow_rebate_history.json'
AUDIT = 'data/dolomite-revenue-onchain-audit.json'
OVERRIDES = 'data/dolomite-revenue-onchain-overrides.json'
FILES = (REVENUE, REBATES, AUDIT, OVERRIDES)
CHAINS = {'Arbitrum','Ethereum','Berachain'}


def check_audit(audit, day):
    if (audit['targetDate'] != day or set(audit['chains']) != CHAINS or
            not all(usable_onchain_revenue_row(row) for row in audit['chains'].values())):
        raise ValueError(f'Incomplete independent onchain revenue evidence for {day}')


def verified(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        if (len(entries) != len(FILES) or set(archive.namelist()) != set(FILES)
                or sum(item.file_size for item in entries)>50*1024**2):
            raise ValueError('Invalid revenue bundle')
        revenue, rebates, audit, overrides = (json.loads(archive.read(name)) for name in FILES)
    stamp = datetime.fromisoformat(revenue['generatedAt'].replace('Z','+00:00'))
    last_day = stamp.date()-timedelta(days=1)
    dates = [row['date'] for row in revenue['series']]
    if dates != sorted(set(dates)) or not all((last_day-timedelta(days=i)).isoformat() in dates for i in range(30)):
        raise ValueError('Incomplete 30-day protocol revenue series')
    check_audit(audit, (stamp.date()-timedelta(days=2)).isoformat())
    if set(rebates['chains']) != {'Arbitrum','Berachain'} or not all(
            row['reconciled'] and not row['missingEpochs'] for row in rebates['chains'].values()):
        raise ValueError('Official rebate allocations could not be reconciled')
    if not overrides['overrides']:
        raise ValueError('Missing historical revenue corrections')
    return stamp


def restore(raw, root):
    verified(raw)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        archive.extractall(root)


def build(target):
    previous = previous_file(BUNDLE)
    if previous:
        restore(previous,ROOT)
    def read(name):
        return json.loads((ROOT/name).read_bytes())
    def run(script,*args):
        subprocess.run([sys.executable,script,*args],cwd=ROOT,check=True)
    old_dates = {row['date'] for row in read(REVENUE)['series']}
    old_corrections = {(row['chain'],row['date']) for row in read(OVERRIDES)['overrides']}
    old_epochs = {c:r['publishedEpoch'] for c,r in read(REBATES)['chains'].items()}
    run('fetch_veborrow_rebate_history.py')
    run('fetch_dolomite_revenue.py')
    audit = read(AUDIT)
    target_day = date.fromisoformat(default_target_date())
    day = date.fromisoformat(audit['targetDate'])
    if day>target_day:
        raise ValueError('Future audit date')
    if day < target_day:
        check_audit(audit,day.isoformat())
    while day < target_day:
        day += timedelta(days=1)
        run('audit_dolomite_revenue_onchain.py','--date',day.isoformat())
        check_audit(read(AUDIT),day.isoformat())
        run('fetch_dolomite_revenue.py')
    run('validate_data.py',AUDIT,REVENUE)
    if not old_dates.issubset({r['date'] for r in read(REVENUE)['series']}):
        raise ValueError('Revenue history regressed')
    if not old_corrections.issubset({(r['chain'],r['date']) for r in read(OVERRIDES)['overrides']}):
        raise ValueError('Historical revenue corrections were dropped')
    if any(read(REBATES)['chains'][chain]['publishedEpoch'] < value for chain,value in old_epochs.items()):
        raise ValueError('Official rebate history regressed')
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        for name in FILES:
            archive.write(ROOT/name,name)
    print('Verified protocol revenue:',verified(target.read_bytes()),'latest audit:',day)


def stage(candidate, output):
    old = previous_file(BUNDLE)
    available = [data for data in (old,candidate.read_bytes() if candidate.is_file() else None) if data is not None]
    if available:
        data = max(available,key=verified)
        restore(data,output)
        (output/BUNDLE).write_bytes(data)


if __name__ == '__main__':
    if sys.argv[1] == 'build':
        build(Path(sys.argv[2]))
    elif sys.argv[1] == 'stage':
        stage(Path(sys.argv[2]),Path(sys.argv[3]))
    else:
        raise SystemExit('Expected build or stage')
