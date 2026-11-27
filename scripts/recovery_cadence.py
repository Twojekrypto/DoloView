"""Catch up delayed scheduled data jobs when another DoloView publication succeeds."""
from datetime import datetime, timezone
import json
import os
import subprocess

CADENCE_HOURS = {
    'publish-borrow-pnl.yml': 2,
    'publish-market-reports.yml': 2,
    'publish-earn-merkl.yml': 1,
    'publish-revenue.yml': 0.5,
    'publish-tvl-details.yml': 0.5,
    'publish-operating-income.yml': 2,
    'publish-earn-r2.yml': 2,
    'publish-vedolo-flows.yml': 2,
    'publish-reward-claims.yml': 2,
    'publish-token-feeds.yml': 2,
    'publish-supply-activity.yml': 1,
    'verify-liquidity.yml': 12,
}


def due(runs, hours, now, *, force=False):
    if any(run['status'] != 'completed' for run in runs):
        return False
    if force or not runs:
        return force
    latest = sorted(runs, key=lambda run: run['createdAt'], reverse=True)
    last_attempt = datetime.fromisoformat(latest[0]['createdAt'].replace('Z', '+00:00'))
    failures = 0
    for run in latest:
        if run.get('conclusion') not in ('failure', 'timed_out'):
            break
        failures += 1
    # A transient failure gets a retry inside the service window. Consecutive
    # failures back off to the normal interval so long scans cannot pile up.
    retry_hours = min(hours, 0.5 * 2 ** (failures - 1)) if failures else hours
    return (now - last_attempt).total_seconds() >= retry_hours * 3600


def main():
    failures = []
    for workflow, hours in CADENCE_HOURS.items():
        force = workflow == 'publish-borrow-pnl.yml' and os.environ.get('FORCE_PNL') == 'true'
        try:
            state = subprocess.check_output(['gh', 'run', 'list', '--workflow', workflow,
                                             '--branch', 'master', '--limit', '5',
                                             '--json', 'status,conclusion,createdAt'])
            if due(json.loads(state), hours, datetime.now(timezone.utc), force=force):
                subprocess.run(['gh', 'workflow', 'run', workflow, '--ref', 'master'], check=True)
                print(f'Requested {workflow}.')
        except subprocess.CalledProcessError:
            failures.append(workflow)
    if failures:
        raise SystemExit(f'Could not check or dispatch collectors: {", ".join(failures)}')


if __name__ == '__main__':
    main()
