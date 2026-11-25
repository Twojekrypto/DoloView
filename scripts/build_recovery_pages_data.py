"""Stage fresh public feeds using the existing, validated Pages producers."""

import argparse
import json
import math
import shutil
from pathlib import Path

from refresh_pages_snapshots import GROUPS, ROOT, refresh_group
from recovery_market_feeds import previous_file


def build(output):
    # A clean checkout is an old seed. Preserve the archive accumulated since
    # cutover before merging the official rolling window or using rate fallback.
    for name in ('assets_live.json', 'dolomite_total_supply_history.json'):
        prior = previous_file(name)
        if prior is not None:
            (ROOT / name).write_bytes(prior)
    asset_file = ROOT / 'assets_live.json'
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
