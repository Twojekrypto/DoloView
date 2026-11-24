"""Restore the verified EARN and Supply Activity seed to an empty directory."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

DATASETS = {
    'earn-snapshots',
    'earn-subaccount-history',
    'earn-verified-ledger',
    'earn-verified-ledger-shards',
    'earn-resolved-interest-ledger',
    'supply-activity',
}


def combine_parts(seed, manifest, archive):
    names = [part['name'] for part in manifest['parts']]
    if not names or names != sorted(set(names)) or any(
        not re.fullmatch(r'data\.tar\.zst\.part-[a-z]{2}', name) for name in names
    ):
        raise ValueError('Invalid archive parts in manifest')
    with archive.open('xb') as combined:
        for part in manifest['parts']:
            path = seed / part['name']
            if path.is_symlink() or path.stat().st_size != part['size']:
                raise ValueError(f'Invalid size or file type for {path.name}')
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for data in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(data)
                    combined.write(data)
            if digest.hexdigest() != part['sha256']:
                raise ValueError(f'Invalid archive checksum for {path.name}')


def unpack(archive, destination, expected_count):
    count = 0
    decoder_program = 'xz' if archive.suffix == '.xz' else 'zstd'
    with subprocess.Popen(
        [decoder_program, '-q', '--decompress', '--stdout', str(archive)],
        stdout=subprocess.PIPE
    ) as decoder:
        try:
            with tarfile.open(fileobj=decoder.stdout, mode='r|') as members:
                for member in members:
                    if member.isdir() and member.name == 'data':
                        continue
                    parts = PurePosixPath(member.name).parts
                    if (len(parts) < 2 or parts[0] != 'data' or parts[1] not in DATASETS
                            or '..' in parts):
                        raise ValueError(f'Unexpected archive path: {member.name}')
                    path = destination.joinpath(*parts)
                    if member.isdir():
                        path.mkdir(parents=True, exist_ok=True)
                        continue
                    if not member.isfile() or len(parts) < 3:
                        raise ValueError(f'Unexpected archive entry type: {member.name}')
                    if count >= expected_count:
                        raise ValueError('Archive has more files than its manifest')
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with members.extractfile(member) as contents, path.open('xb') as target:
                        shutil.copyfileobj(contents, target)
                    count += 1
            with open(os.devnull, 'wb') as sink:
                shutil.copyfileobj(decoder.stdout, sink)
        finally:
            decoder.stdout.close()
        if decoder.wait() != 0:
            raise ValueError('Cannot decompress the verified seed')
    if count != expected_count:
        raise ValueError(f'Archive file count {count} differs from manifest {expected_count}')


def restore(seed, output):
    manifest = json.loads((seed / 'manifest.json').read_text())
    if manifest.get('version') != 1 or manifest.get('format') != 'tar.zst-split':
        raise ValueError('Unsupported seed manifest')
    count = manifest['files']
    if type(count) is not int or count <= 0:
        raise ValueError('Invalid file count in seed manifest')
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix='.seed-restore-') as directory:
        root = Path(directory)
        archive = root / 'data.tar.zst'
        combine_parts(seed, manifest, archive)
        destination = root / 'data-root'
        unpack(archive, destination, count)
        destination.rename(output)
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('seed_dir', type=Path)
    parser.add_argument('output_dir', type=Path)
    options = parser.parse_args()
    restored = restore(options.seed_dir, options.output_dir)
    print(f'Restored {restored:,} files from verified archive parts')
