"""Stage fresh public feeds using the existing, validated Pages producers."""

import argparse
import shutil
from pathlib import Path

from refresh_pages_snapshots import GROUPS, ROOT, refresh_group


def build(output):
    statuses = [refresh_group(name, paths, commands) for name, paths, commands in GROUPS]
    if not all(statuses):
        raise ValueError("At least one feed failed its freshness or schema checks")
    output.mkdir(parents=True, exist_ok=False)
    for _, paths, _ in GROUPS:
        for filename in paths:
            shutil.copy2(ROOT / filename, output / filename)
    (output / ".nojekyll").touch()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
