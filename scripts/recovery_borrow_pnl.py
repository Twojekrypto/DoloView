"""Publish public 30-day P&L snapshots independently of the private oracle cache."""
import argparse
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import zipfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT

NAME = 'verified-borrow-pnl.zip'
PARENT = 'borrow_position_pnl.json'
ACTIVITY = 'data/borrow-position-pnl-history'
SHARD = re.compile(ACTIVITY + r'/[0-9a-f]{24}/(?:ethereum|arbitrum|berachain)-[0-9a-f]\.json')


def expand(data, root):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        names = {i.filename for i in entries}
        if (len(names) != len(entries) or PARENT not in names or not 1 < len(names) <= 5000 or
                sum(i.file_size for i in entries)>600*1024**2 or any(n!=PARENT and not SHARD.fullmatch(n) for n in names)):
            raise ValueError('Invalid public P&L bundle')
        for item in entries:
            path=root/item.filename
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(archive.read(item))


def cutoff(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        return json.loads(archive.read(PARENT))['sourceAsOf']


def needs_collection(candidate, now):
    # Reserve the publisher's 32-minute build window before the six-hour limit.
    def fresh(data):
        return 0 <= now - cutoff(data) <= (360 - 32) * 60
    if candidate.is_file() and fresh(candidate.read_bytes()):
        return False
    published = previous_file(NAME)
    return published is None or not fresh(published)


def build(path):
    baseline=previous_file(NAME)
    if baseline:
        expand(baseline,ROOT)
    subprocess.run([sys.executable,'fetch_borrow_position_pnl.py','--cache','.cache/borrow-position-pnl',
                    '--output',PARENT,'--activity-dir',ACTIVITY],cwd=ROOT,check=True)
    subprocess.run([sys.executable,'build_borrow_position_pnl.py','--validate',PARENT],cwd=ROOT,check=True)
    snapshot=json.loads((ROOT/PARENT).read_bytes())
    if not 0 <= time.time()-snapshot['sourceAsOf'] <= snapshot['maxAgeSeconds'] or not snapshot.get('activity',{}).get('files'):
        raise ValueError('P&L origin is stale or missing its verified position history')
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED,compresslevel=8) as archive:
        archive.write(ROOT/PARENT,PARENT)
        for file in sorted((ROOT/ACTIVITY).glob('*/*.json')):
            name=file.relative_to(ROOT).as_posix()
            if not SHARD.fullmatch(name):
                raise ValueError('Unexpected P&L history name')
            archive.write(file,name)
    print(f"Verified {snapshot['summary']['verifiedPnl']:,} positions, compressed public bundle {path.stat().st_size:,} bytes")


def stage(candidate, output):
    old=previous_file(NAME)
    data=candidate.read_bytes() if candidate.is_file() else old
    if data is None:
        raise ValueError("No verified public P&L is available to publish")
    if old and cutoff(old)>cutoff(data):
        data=old
    expand(data,output)
    subprocess.run([sys.executable,str(ROOT/'build_borrow_position_pnl.py'),'--validate',str(output/PARENT)],check=True)
    (output/NAME).write_bytes(data)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['build','stage','check-age'])
    parser.add_argument('bundle',type=Path)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if args.mode=='build':
        build(args.bundle)
    elif args.mode=='check-age':
        print('true' if needs_collection(args.bundle,time.time()) else 'false')
    else:
        stage(args.bundle,args.output)
