import datetime as dt
from contextlib import redirect_stdout
from io import StringIO
import fetch_dolo_price_history as price_history
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1] / 'scripts'))
import recovery_market_feeds as feeds


class MarketFeedTests(unittest.TestCase):
    def test_chart_requests_at_most_500_points_and_reaches_last_day(self):
        day = 86400
        start = price_history.START_TS
        end = start + 582 * day + day // 2
        with patch.object(price_history.time, 'time', return_value=end), \
                patch.object(price_history, 'fetch_window', side_effect=[{'old': 1}, {'last': 2}]) as fetch:
            self.assertEqual(price_history.fetch_chart(start), {'old': 1, 'last': 2})
        self.assertEqual([call.args for call in fetch.call_args_list],
                         [(start + day // 2, 500), (start + day // 2 + 500 * day, 83)])

    def test_price_needs_recent_quote_and_complete_history(self):
        day = dt.date(2026, 11, 25)
        dump = lambda values: json.dumps({'prices':dict.fromkeys(values,1)}).encode()
        old = dump(['2026-11-23'])
        feeds.validate_price(old,dump(['2026-11-23','2026-11-24']),day)
        for data in [old,dump(['2026-11-25']),dump(['2026-11-23','2026-11-25']),dump(['2026-11-23','2026-11-24','2026-11-25','2026-11-26'])]:
            with self.assertRaises(ValueError):
                feeds.validate_price(old,data,day)

    def test_sources_fail_independently_and_retain_published_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            previous = {feeds.PRICE: b'{"prices":{"2026-01-01":1}}',feeds.ICONS: b'/* registry */',
                        feeds.WLFI:b'{"source":{"blockNumber":50}}'}
            for name in previous:
                path = root / name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(b'old checkout')
            def run(command, **kwargs):
                if 'fetch_dolo_price_history.py' in command:
                    (root/feeds.PRICE).write_text('{"prices":{}}')
                if 'generate_wlfi_markets.py' in command:
                    (root/feeds.WLFI).write_text('{"source":{"blockNumber":51}}')
            with patch.object(feeds,'ROOT',root),patch.object(feeds,'previous_file',previous.get), \
                 patch.object(feeds.subprocess,'run',run),patch.object(feeds,'check_freshness'), redirect_stdout(StringIO()):
                self.assertEqual(feeds.build(root/'public'),[feeds.PRICE])
            self.assertEqual((root/'public'/feeds.PRICE).read_bytes(),previous[feeds.PRICE])
            self.assertEqual(json.loads((root/'public'/feeds.WLFI).read_text())['source']['blockNumber'],51)
