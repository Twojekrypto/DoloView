"""Stage the borrow dataset as one generation using the existing scanners and validators."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from recovery_market_feeds import previous_file
from refresh_pages_snapshots import ROOT, check_freshness
sys.path.insert(0, str(ROOT))
from build_borrow_earnings import CHAINS, sha256
from fetch_liquidation_risk import write_liquidation_risk_shards

RISK = 'liquidation_risk.json'
HISTORY = 'liquidation_history.json'
EARNINGS = 'borrow_earnings.json'
COUNTS = 'data/liquidation-risk/position-count-history.json'
FILES = (RISK, HISTORY, EARNINGS, COUNTS)


def validate(risk, history, earnings):
    if not risk.get('positions') or risk.get('staleChains') or not set(CHAINS).issubset(risk['chainStats']):
        raise ValueError('Missing current borrow positions or history for an active network')
    if history['generatedAt'] != risk['generatedAt'] or not history['liquidationHistory']:
        raise ValueError('Liquidation history does not match positions')
    if earnings['sources']['positionsPayloadSha256'] != sha256(risk):
        raise ValueError('APR ranking does not match the positions')
    if not earnings.get('positions') or any(earnings['chainStatus'].get(c, {}).get('status') != 'ok' for c in CHAINS):
        raise ValueError('Current APR needs verified rate coverage for all three networks')


def stage(output):
    with tempfile.TemporaryDirectory() as directory:
        folder = Path(directory)
        for name in FILES:
            p = folder / name
            p.parent.mkdir(parents=True, exist_ok=True)
            data = previous_file(name)
            p.write_bytes(data if data is not None else (ROOT / name).read_bytes())
        baseline = {name:(folder/name).read_bytes() for name in FILES}
        def run(script, *args):
            subprocess.run([sys.executable, str(ROOT / script), *args], cwd=folder, check=True, timeout=800)
        error = None
        try:
            run('fetch_liquidation_risk.py')
            risk = json.loads((folder/RISK).read_bytes())
            write_liquidation_risk_shards(risk, folder/'data/liquidation-risk')
            run('validate_data.py', RISK, HISTORY, 'data/liquidation-risk')
            run('build_borrow_earnings.py', '--risk', RISK, '--output', EARNINGS)
            validate(risk, json.loads((folder/HISTORY).read_bytes()), json.loads((folder/EARNINGS).read_bytes()))
            check_freshness([RISK, HISTORY, EARNINGS], folder)
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
            error = str(exc)
            for name, raw in baseline.items():
                (folder/name).write_bytes(raw)
            shutil.rmtree(folder/'data/liquidation-risk')
            write_liquidation_risk_shards(json.loads(baseline[RISK]), folder/'data/liquidation-risk')
            (folder/COUNTS).write_bytes(baseline[COUNTS])
            print('::error::Borrow refresh failed, previous generation retained: ' + error)
        for name in FILES[:3]:
            shutil.copyfile(folder/name, output/name)
        shutil.copytree(folder/'data/liquidation-risk', output/'data/liquidation-risk')
        return [RISK, HISTORY, EARNINGS] if error else []


if __name__ == '__main__':
    errors = Path(sys.argv[2])
    errors.write_text(json.dumps(json.loads(errors.read_text()) + stage(Path(sys.argv[1]))))
