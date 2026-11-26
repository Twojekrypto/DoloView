"""Stage the most recent complete operating income calculation without modifying its dates."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT

sys.path.insert(0,str(ROOT))
from fetch_operating_income import build_snapshot

NAME = 'data/operating-income/latest.json'


def verified_time(raw):
    source = json.loads(raw)
    stamp = datetime.fromisoformat(source['generatedAt'].replace('Z','+00:00'))
    if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
        raise ValueError('Invalid source date for operating income')
    treasury = source['treasury']
    liquidations = dict(source['liquidations'])
    # The canonical calculator adds readable dates to the raw liquidation window.
    liquidations['period'] = {key: liquidations['period'][key] for key in ('startTimestamp','endTimestamp')}
    if build_snapshot(treasury, liquidations, stamp) != source:
        raise ValueError('Operating income totals or source windows do not reconcile')
    return stamp


def stage(candidate, output):
    old = previous_file(NAME)
    options = [data for data in (old, candidate.read_bytes() if candidate.is_file() else None) if data is not None]
    if options:
        data = max(options, key=verified_time)
        path = output/NAME
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(data)


if __name__ == '__main__':
    if sys.argv[1] == 'verify':
        stamp = verified_time(Path(sys.argv[2]).read_bytes())
        if (datetime.now(timezone.utc)-stamp).total_seconds() > 40*60:
            raise ValueError('Operating income was not refreshed in this run')
        print(stamp.isoformat())
    elif sys.argv[1] == 'stage':
        stage(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        raise SystemExit('Expected verify or stage')
