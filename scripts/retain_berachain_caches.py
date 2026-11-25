"""Keep two restorable checkpoints per Berachain scanner and GitHub cache version."""
import argparse
from collections import defaultdict
from functools import lru_cache
import json
import re
import subprocess

REPO = "Twojekrypto/Dolomite-dashboard"
REF = "refs/heads/master"
KEY = re.compile(r"(earn-canonical-(?:global-)?runtime-berachain)-(\d+)")
SAVES = {
    "earn-canonical-runtime-berachain": "Save Berachain canonical runtime cache",
    "earn-canonical-global-runtime-berachain": "Save Berachain global runtime cache",
}


def api(path, *, pages=False):
    flags = ["--paginate", "--slurp"] if pages else []
    return json.loads(subprocess.check_output(["gh", "api", *flags, f"repos/{REPO}/{path}"]))


def groups():
    result = defaultdict(list)
    for page in api("actions/caches?per_page=100", pages=True):
        for item in page["actions_caches"]:
            match = KEY.fullmatch(item["key"])
            if match and item["ref"] == REF and item["size_in_bytes"] > 0:
                result[(match[1], item["version"])].append(item)
    return result


@lru_cache(maxsize=None)
def verified(key):
    family, run_id = KEY.fullmatch(key).groups()
    run = api(f"actions/runs/{run_id}")
    if run["status"] != "completed" or run["head_branch"] != "master":
        return False
    for page in api(f"actions/runs/{run_id}/jobs?per_page=100", pages=True):
        for job in page["jobs"]:
            if any(step["name"] == SAVES[family] and step["conclusion"] == "success"
                   for step in job.get("steps", [])):
                return True
    return False


def retained_and_expired(items, verify):
    ordered = sorted(items, key=lambda x: (x["created_at"], x["id"]), reverse=True)
    if len(ordered) <= 2 or not all(verify(x["key"]) for x in ordered[:2]):
        return [], []
    return ordered[:2], ordered[2:]


def prune(apply):
    removed = 0
    for group, items in groups().items():
        protected, candidates = retained_and_expired(items, verified)
        if not candidates:
            continue
        protected_ids = {row["id"] for row in protected}
        for row in candidates:
            print(f"{group[0]}: {row['key']} ({row['size_in_bytes']:,} bytes)", flush=True)
            if not apply:
                continue
            latest = groups().get(group, [])
            keep, removable = retained_and_expired(latest, verified)
            if not protected_ids.issubset({r["id"] for r in latest}) or not keep:
                raise RuntimeError("The protected caches changed; retry with a fresh inventory")
            if row["id"] not in {r["id"] for r in removable}:
                continue
            subprocess.run(["gh", "api", "--method", "DELETE",
                            f"repos/{REPO}/actions/caches/{row['id']}"], check=True)
            remaining = {r["id"] for r in groups().get(group, [])}
            if row["id"] in remaining or not {r["id"] for r in keep}.issubset(remaining):
                raise RuntimeError("Unexpected cache inventory after deletion")
            removed += row["size_in_bytes"]
    if apply:
        print(f"Removed {removed:,} bytes", flush=True)


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--apply", action="store_true")
    prune(cli.parse_args().apply)
