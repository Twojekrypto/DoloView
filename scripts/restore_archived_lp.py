"""Recover a single, byte-identical historical LP snapshot from its private archive."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.data_artifact_store import checked_digest, configured_store


def extract_snapshot(archive, archive_sha, digest):
    digest = checked_digest(digest)
    if hashlib.sha256(archive).hexdigest() != checked_digest(archive_sha):
        raise ValueError("Unexpected archive checksum")
    obj = f"lp/v1/objects/{digest}.json"
    version = f"lp/v1/versions/{digest}.json"
    parts = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r|xz") as reader:
        for item in reader:
            if item.name in (obj, version):
                if not item.isfile() or item.name in parts or not 0 < item.size <= 16_000_000:
                    raise ValueError("Invalid archive entry")
                parts[item.name] = reader.extractfile(item).read()
    if set(parts) != {obj, version}:
        raise ValueError("Snapshot or version manifest missing from archive")
    body = parts[obj]
    manifest = json.loads(parts[version])
    if manifest["sha256"] != digest or hashlib.sha256(body).hexdigest() != digest or manifest["size"] != len(body):
        raise ValueError("Snapshot does not match its stored version")
    return manifest, body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="Local tar.xz. If omitted, use the private R2 archive.")
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--digest", required=True, help="Original LP version SHA-256")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output", type=Path, help="Create a local JSON file")
    target.add_argument("--rehydrate", action="store_true", help="Restore the original object for legacy rollback")
    args = parser.parse_args()
    archive_sha = checked_digest(args.archive_sha256)
    digest = checked_digest(args.digest)
    store = configured_store() if args.rehydrate or not args.archive else None
    if args.archive:
        archive = args.archive.read_bytes()
    else:
        archive = store.client.get_object(Bucket=store.bucket, Key=f"lp-archives/v1/{archive_sha}.tar.xz")["Body"].read()
    manifest, body = extract_snapshot(archive, archive_sha, digest)
    if args.rehydrate:
        version, _ = store._json(f"versions/{digest}.json")
        if version != manifest:
            raise ValueError("The live immutable version manifest differs")
        store._put(f"objects/{digest}.json", body, immutable=True)
        if store._get(f"objects/{digest}.json")[0] != body:
            raise ValueError("R2 readback differs")
    else:
        with args.output.open("xb") as f:
            f.write(body)
    print(f"Recovered {len(body)} bytes, SHA-256 {digest}.")


if __name__ == "__main__":
    main()
