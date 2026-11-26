"""Stage an append-validated veDOLO event history without committing generated data."""
import argparse
import json
from pathlib import Path
import sys

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness

sys.path.insert(0, str(ROOT))
from generate_vedolo_flows import assert_complete_transfer_history, assert_no_immutable_event_regression


FILE = 'vedolo_flows.json'


def validate(raw, previous=None):
    history = json.loads(raw)
    block = history['target_block']
    if type(block) is not int or block <= 0 or history['transfers_schema_version'] != 2:
        raise ValueError('Invalid veDOLO history checkpoint or transfer schema')
    for group in ('locks', 'unlocks', 'transfers'):
        events = history[group]
        if not isinstance(events, list) or len(events) != history['total_' + group] or not events:
            raise ValueError(f'Incomplete {group} in veDOLO history')
        if any(not 0 < int(row['block']) <= block for row in events):
            raise ValueError(f'{group} outside the verified veDOLO block range')
    assert_complete_transfer_history(history['transfers'], block)
    if previous is not None:
        if block < previous['target_block']:
            raise ValueError('veDOLO checkpoint regressed')
        assert_no_immutable_event_regression(previous, history, block)
    return history


def synchronize(candidate, output, mode):
    seed = (ROOT / FILE).read_bytes()
    previous = previous_file(FILE)
    previous_history = validate(previous, json.loads(seed)) if previous is not None else None
    if mode == 'prepare':
        if previous is not None:
            (ROOT / FILE).write_bytes(previous)
        return
    if mode == 'verify':
        if json.loads((ROOT / 'vedolo_flows_run_status.json').read_bytes())['completed'] is not True:
            raise ValueError('veDOLO scan is incomplete; retaining the published history')
        validate((ROOT / FILE).read_bytes(), previous_history)
        check_freshness([FILE], ROOT)
        return

    selected = previous
    if candidate.is_file():
        new = candidate.read_bytes()
        new_history = validate(new, json.loads(seed))
        if previous_history is None or new_history['target_block'] >= previous_history['target_block']:
            validate(new, previous_history)
            selected = new
    if selected is not None:
        (output / FILE).write_bytes(selected)
        print(f"Staged veDOLO event history through block {json.loads(selected)['target_block']:,}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'verify', 'stage'])
    parser.add_argument('--candidate', type=Path, default=Path('verified-vedolo/' + FILE))
    parser.add_argument('--output', type=Path, default=Path('_data'))
    args = parser.parse_args()
    synchronize(args.candidate, args.output, args.mode)
