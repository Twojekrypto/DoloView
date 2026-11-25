"""Publish the remaining price and asset files and the reconciled Ethereum WLFI snapshot."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import requests

from refresh_pages_snapshots import ROOT, check_freshness

BASE = 'https://twojekrypto.github.io/DoloView/'
PRICE = 'dolo_price_history.json'
ICONS = 'dolomite-token-icons.generated.js'
WLFI = 'data/wlfi-markets/latest.json'


def previous_file(name):
    for attempt in range(3):
        try:
            response = requests.get(BASE + name, params={'recovery': int(time.time())},
                                    timeout=25, allow_redirects=False)
            if response.status_code == 404:
                return None
            if response.status_code != 200:
                raise RuntimeError(f'Cannot restore previously published {name}: HTTP {response.status_code}')
            return response.content
        except (requests.RequestException, RuntimeError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def validate_price(old, new, today):
    previous = json.loads(old)['prices']
    prices = json.loads(new)['prices']
    if not previous or not set(previous).issubset(prices):
        raise ValueError('Historical DOLO prices were dropped')
    first, last = map(dt.date.fromisoformat, (min(prices), max(prices)))
    if last < first or len(prices) != (last-first).days + 1 or any(
            dt.date.fromisoformat(day).isoformat() != day for day in prices):
        raise ValueError('Gap in daily DOLO prices')
    if max(prices) < (today - dt.timedelta(days=1)).isoformat() or max(prices) > today.isoformat():
        raise ValueError('Price feed has no recent daily quote')


def build(output):
    sources = (
        (PRICE, [sys.executable, 'fetch_dolo_price_history.py']),
        (ICONS, ['node', 'scripts/sync_dolomite_token_icons.mjs']),
        (WLFI, [sys.executable, 'generate_wlfi_markets.py']),
    )
    failed = []
    for name, command in sources:
        target = ROOT / name
        prior = previous_file(name)
        if prior is not None:
            target.write_bytes(prior)
        baseline = target.read_bytes()
        try:
            subprocess.run(command, cwd=ROOT, timeout=220, check=True)
            if name == PRICE:
                validate_price(baseline, target.read_bytes(), dt.datetime.now(dt.timezone.utc).date())
                subprocess.run([sys.executable, 'validate_data.py', PRICE], cwd=ROOT, check=True)
            elif name == ICONS:
                if target.stat().st_size == 0:
                    raise ValueError('Empty token icon registry')
                subprocess.run(['node', '--check', ICONS], cwd=ROOT, check=True)
            else:
                check_freshness([WLFI], ROOT)
                previous = json.loads(baseline)
                current = json.loads(target.read_bytes())
                if current['source']['blockNumber'] < previous['source']['blockNumber']:
                    raise ValueError('WLFI block regressed')
        except (subprocess.SubprocessError, OSError, ValueError, KeyError) as error:
            failed.append(name)
            target.write_bytes(baseline)
            print(f'::error::{name} refresh failed, preserving prior bytes: {error}')
        staged = output / name
        staged.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(target, staged)
        print(f'{name} {hashlib.sha256(staged.read_bytes()).hexdigest()}')
    return failed


if __name__ == '__main__':
    failed = build(Path(sys.argv[1]))
    Path(sys.argv[2]).write_text(json.dumps(failed))
