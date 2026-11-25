"""Stage fresh public feeds using the existing, validated Pages producers."""

import argparse
import json
import math
import shutil
from pathlib import Path

from refresh_pages_snapshots import GROUPS, ROOT, refresh_group
from recovery_market_feeds import previous_file


def build(output):
    # Rate fallback may need the prior public feed even on a clean runner.
    asset_file = ROOT / 'assets_live.json'
    prior = previous_file('assets_live.json')
    if prior is not None:
        asset_file.write_bytes(prior)
    statuses = [refresh_group(name, paths, commands) for name, paths, commands in GROUPS]
    if not all(statuses):
        raise ValueError("At least one feed failed its freshness or schema checks")
    assets = json.loads(asset_file.read_bytes())
    if assets.get('rateFallbackChains'):
        age = assets.get('rateFallbackMaxAgeMinutes')
        if type(age) not in (int, float) or not math.isfinite(age) or not 0 <= age <= 360:
            raise ValueError('Cached asset rates exceed the six-hour publication limit')
    output.mkdir(parents=True, exist_ok=False)
    for _, paths, _ in GROUPS:
        for filename in paths:
            shutil.copy2(ROOT / filename, output / filename)
    (output / ".nojekyll").touch()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
