"""Pack the large data directories into independently reusable lossless archives."""

import argparse
import hashlib
import json
import re
import subprocess
import tarfile
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from scripts.restore_recovery_seed import DATASETS, unpack

LEGACY_FORMAT = 'zstd-tar-by-dataset'
FORMAT = 'tar-by-dataset'
SHA256 = re.compile(r'[a-f0-9]{64}')
DATE_FILE = re.compile(r'\d{4}-\d{2}-\d{2}\.json')
ADDRESS = re.compile(r'0x([a-f0-9]+)')
HEX_SHARD = re.compile(r'[a-f0-9]{2}\.json')
ARCHIVE_LIMIT = 128 * 1024 * 1024


def chunk_for(path):
    _, dataset, *parts = path.parts
    name = parts[-1]
    if dataset == 'earn-snapshots' and DATE_FILE.fullmatch(name):
        return f'{dataset}/{name[:7]}'
    if len(parts) == 1:
        return f'{dataset}/index'
    chain = parts[0]
    if dataset == 'supply-activity':
        if chain == 'recent' and len(parts) > 2:
            return f'{dataset}/recent/{parts[1]}'
        market = ADDRESS.match(name)
        return f'{dataset}/{chain}/{market.group(0) if market else "index"}'
    address = ADDRESS.match(name)
    if address:
        return f'{dataset}/{chain}/{address.group(1)[0]}'
    if HEX_SHARD.fullmatch(name):
        return f'{dataset}/{chain}/{name[0]}'
    return f'{dataset}/{chain}/index'


def file_digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(data)
    return digest.hexdigest()


def collect(root, datasets=DATASETS):
    groups = defaultdict(list)
    for dataset in sorted(datasets):
        target = root / 'data' / dataset
        if target.is_symlink():
            raise ValueError(f'Unexpected symlink: {target}')
        for path in sorted(target.rglob('*')):
            if path.is_symlink():
                raise ValueError(f'Unexpected symlink: {path}')
            if path.is_file():
                relative = path.relative_to(root)
                groups[chunk_for(relative)].append((path, relative.as_posix(), path.stat().st_size))
    if not groups:
        raise ValueError('No supported dataset files found')
    return groups


def archive_entries(entries, staging):
    raw = staging / 'current.tar'
    with tarfile.open(raw, mode='w', format=tarfile.PAX_FORMAT) as tar:
        for path, name, size in entries:
            item = tarfile.TarInfo(name)
            item.size = size
            with path.open('rb') as contents:
                tar.addfile(item, contents)
    # Choose per chunk. Small JSON indexes and large runs of similar daily
    # snapshots compress differently. Both encodings restore an ordinary tar.
    commands = [
        ('quick.zst', ['zstd', '-q', '-3', '-c']),
        ('dense.zst', ['zstd', '-q', '--ultra', '-22', '--long=26', '--single-thread', '-c']),
        ('dense.xz', ['xz', '-9e', '--threads=1', '-c']),
    ]
    with tempfile.TemporaryDirectory(dir=staging) as directory:
        candidates = []
        for filename, command in commands:
            candidate = Path(directory) / filename
            with candidate.open('xb') as compressed:
                subprocess.run([*command, raw], stdout=compressed, check=True)
            candidates.append(candidate)
        best = min(candidates, key=lambda p: p.stat().st_size)
        if best.stat().st_size > ARCHIVE_LIMIT:
            raise ValueError('Compressed data chunk exceeds 128 MiB')
        archive = staging / ('current.tar' + best.suffix)
        best.replace(archive)
    raw.unlink()
    return archive


def check_manifest(manifest):
    version = manifest.get('version')
    if (version, manifest.get('format')) not in ((1, LEGACY_FORMAT), (2, FORMAT)):
        raise ValueError('Unsupported data pack manifest')
    if not manifest['chunks'] or sum(item['files'] for item in manifest['chunks'].values()) != manifest['files']:
        raise ValueError('Manifest file count does not match its chunks')
    for item in manifest['chunks'].values():
        if version == 1 and item.get('codec', 'zst') != 'zst':
            raise ValueError('Invalid codec in legacy manifest')
        if version == 2 and item.get('codec') not in ('zst', 'xz'):
            raise ValueError('Invalid codec in data pack manifest')
        if (not SHA256.fullmatch(item['sha256']) or not SHA256.fullmatch(item['contentSha256'])
                or not 0 < item['bytes'] <= ARCHIVE_LIMIT or item['files'] <= 0):
            raise ValueError('Invalid chunk record in manifest')
    return manifest


def pack(root, output, previous=None, *, datasets=DATASETS, workers=1):
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    previous_chunks = check_manifest(previous)['chunks'] if previous is not None else {}
    groups = collect(root, datasets)
    missing = {name.partition('/')[0] for name in previous_chunks} - {
        name.partition('/')[0] for name in groups
    }
    if missing:
        raise ValueError(f'Input is missing datasets from the previous manifest: {sorted(missing)}')
    chunks = {}
    with tempfile.TemporaryDirectory(dir=output.parent, prefix='.data-pack-') as temporary:
        staging = Path(temporary) / 'stage'
        objects = staging / 'objects'
        objects.mkdir(parents=True)
        def build_chunk(pair):
            name, entries = pair
            signature = [(relative, size, file_digest(path)) for path, relative, size in entries]
            content_hash = hashlib.sha256(json.dumps(signature, separators=(',', ':')).encode()).hexdigest()
            cached = previous_chunks.get(name)
            if cached and cached['contentSha256'] == content_hash and cached['files'] == len(entries):
                return name, {**cached, 'codec': cached.get('codec', 'zst')}
            with tempfile.TemporaryDirectory(dir=staging) as work:
                archive = archive_entries(entries, Path(work))
                stored_hash = file_digest(archive)
                record = {'files': len(entries), 'contentSha256': content_hash,
                          'sha256': stored_hash, 'bytes': archive.stat().st_size,
                          'codec': archive.suffix[1:]}
                archive.replace(objects / f'{stored_hash}.tar{archive.suffix}')
            return name, record

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for index, (name, record) in enumerate(pool.map(build_chunk, sorted(groups.items())),1):
                chunks[name] = record
                if workers > 1 and (index % 25 == 0 or index == len(groups)):
                    print(f"Checked {index}/{len(groups)} archive groups", flush=True)
        manifest = {'version': 2, 'format': FORMAT,
                    'files': sum(len(entries) for entries in groups.values()), 'chunks': chunks}
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        staging.rename(output)
    return manifest


def restore(packed, output, *, datasets=None):
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    manifest = check_manifest(json.loads((packed / 'manifest.json').read_text()))
    present = {name.partition('/')[0] for name in manifest['chunks']}
    requested = set(datasets or present)
    if missing := requested - present:
        raise ValueError(f'Unknown datasets in requested restore: {sorted(missing)}')
    chunks = [item for name, item in manifest['chunks'].items()
              if name.partition('/')[0] in requested]
    with tempfile.TemporaryDirectory(dir=output.parent, prefix='.data-unpack-') as temporary:
        destination = Path(temporary) / 'stage'
        for item in chunks:
            archive = packed / 'objects' / f"{item['sha256']}.tar.{item.get('codec', 'zst')}"
            if (archive.is_symlink() or archive.stat().st_size != item['bytes']
                    or file_digest(archive) != item['sha256']):
                raise ValueError(f'Invalid archive checksum or size: {archive.name}')
            unpack(archive, destination, item['files'])
        destination.rename(output)
    return sum(item['files'] for item in chunks)


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('operation', choices=('pack', 'restore'))
    cli.add_argument('source', type=Path)
    cli.add_argument('output', type=Path)
    cli.add_argument('--previous-manifest', type=Path)
    cli.add_argument('--dataset', action='append', choices=sorted(DATASETS))
    args = cli.parse_args()
    if args.operation == 'pack':
        previous = json.loads(args.previous_manifest.read_text()) if args.previous_manifest else None
        result = pack(args.source, args.output, previous)
        print(f"Packed {result['files']:,} files into {len(result['chunks']):,} chunks")
    else:
        print(f"Restored {restore(args.source, args.output, datasets=args.dataset):,} files")
