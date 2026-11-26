"""Catch up delayed scheduled data jobs when another DoloView publication succeeds."""
from datetime import datetime, timezone
import json
import os
import subprocess

CADENCE_HOURS = {
    'publish-borrow-pnl.yml': 2,
    'publish-market-reports.yml': 4,
    'publish-earn-merkl.yml': 1,
    'publish-earn-r2.yml': 6,
}


def due(runs, hours, now):
    if not runs or any(run['status'] != 'completed' for run in runs):
        return False
    last_attempt = max(datetime.fromisoformat(run['createdAt'].replace('Z', '+00:00')) for run in runs)
    return (now - last_attempt).total_seconds() >= hours * 3600


def main():
    for workflow, hours in CADENCE_HOURS.items():
        if workflow == 'publish-borrow-pnl.yml' and os.environ.get('PNL_COLLECTED_HERE') == 'success':
            continue
        state = subprocess.check_output(['gh', 'run', 'list', '--workflow', workflow, '--branch', 'master',
                                         '--limit', '5', '--json', 'status,createdAt'])
        if due(json.loads(state), hours, datetime.now(timezone.utc)):
            subprocess.run(['gh', 'workflow', 'run', workflow, '--ref', 'master'], check=True)
            print(f'Requested {workflow} after its {hours}-hour interval.')


if __name__ == '__main__':
    main()
