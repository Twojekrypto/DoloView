"""Resume the DOLO liquidity collector from its validated public snapshot."""
import argparse
import subprocess
import sys
from pathlib import Path

from data_artifact_store import timestamp, validate_payload
from recovery_market_feeds import previous_file
from refresh_pages_snapshots import check_freshness

FILE = 'data/dolo-liquidity.json'
PUBLISHED_SNAPSHOT = 'https://doloview.pages.dev/'


def verify_transition(before, after):
    previous = validate_payload(before, 'resume')
    current = validate_payload(after, 'current')
    if timestamp(current['generatedAt']) <= timestamp(previous['generatedAt']):
        raise ValueError('The liquidity scan has not advanced')
    blocks = {row['key']: row['lastScannedBlock'] for row in current['sources']}
    for row in previous['sources']:
        if row['key'] not in blocks or blocks[row['key']] < row['lastScannedBlock']:
            raise ValueError(f'Liquidity coverage regressed for {row["key"]}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'verify'])
    parser.add_argument('baseline', type=Path)
    args = parser.parse_args()
    if args.mode == 'prepare':
        body = previous_file(FILE)
        if body is None:
            body = previous_file(FILE, PUBLISHED_SNAPSHOT)
        if body is None:
            raise ValueError('No published liquidity baseline is available')
        validate_payload(body, 'resume')
        price = previous_file('dolo_price.json')
        quote = Path('dolo_price.json')
        if price is not None:
            quote.write_bytes(price)
        try:
            check_freshness([str(quote)], Path.cwd())
        except (FileNotFoundError, ValueError):
            subprocess.run([sys.executable, 'update_dolo_price.py'], check=True, timeout=120)
            check_freshness([str(quote)], Path.cwd())
        args.baseline.write_bytes(body)
        Path(FILE).write_bytes(body)
    else:
        verify_transition(args.baseline.read_bytes(), Path(FILE).read_bytes())
