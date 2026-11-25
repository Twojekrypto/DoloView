import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import earn_recovery_store as store
from scripts.recovery_data_packs import pack


class Response:
    def __init__(self, value):
        self.content = value
        self.headers = {'ETag': '"' + hashlib.sha256(value).hexdigest() + '"'}

    def json(self):
        return json.loads(self.content)


class StorageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'source'
        for dataset in store.DATASETS:
            p = self.source / 'data' / dataset / 'manifest.json'
            p.parent.mkdir(parents=True)
            p.write_text(json.dumps({'dataset': dataset}))
        initial = self.root / 'base'
        baseline = pack(self.source, initial, datasets=store.DATASETS)
        body = store.encode(baseline)
        self.data = {'/base': body}
        for item in baseline['chunks'].values():
            name = store.filename(item)
            self.data['/base/' + name] = (initial / 'objects' / name).read_bytes()
        for mocked in (patch.object(store, 'request', self.request),
                       patch.object(store, 'SEED_HASH', hashlib.sha256(body).hexdigest())):
            mocked.start()
            self.addCleanup(mocked.stop)
        self.checkout = self.root / 'checkout'
        (self.checkout / 'data').mkdir(parents=True)
        self.state = self.root / 'state'
        store.hydrate(self.checkout, self.state)

    def request(self, method, path, *, optional=False, data=None, headers=None):
        value = self.data.get(path)
        if method in ('GET', 'HEAD'):
            if value is None and optional: return None
            if value is None: raise KeyError(path)
            return Response(value)
        self.assertEqual(headers['X-Content-Sha256'], hashlib.sha256(data).hexdigest())
        if value is None:
            self.assertEqual(headers['If-None-Match'], '*')
        else:
            self.assertEqual(headers['If-Match'], Response(value).headers['ETag'])
        self.data[path] = data
        return Response(b'')

    def test_roundtrip_reuse_and_alternating_slots(self):
        target = self.checkout / 'data/earn-snapshots/manifest.json'
        name = 'earn-snapshots/index'
        for index in range(3):
            current, etag = store.read_current()
            self.state.write_bytes(store.encode({'manifest': current, 'etag': etag}))
            target.write_text(str(index))
            store.publish(self.checkout, self.state)
            after, _ = store.read_current()
            self.assertEqual(after['chunks'][name]['slot'], index % 2)
        self.assertEqual(sum(k.startswith('/slots/') for k in self.data), 2)
        new = self.root / 'new'
        (new / 'data').mkdir(parents=True)
        store.hydrate(new, self.state)
        self.assertEqual((new / 'data/earn-snapshots/manifest.json').read_text(), '2')
        saved = self.data.copy()
        store.publish(new, self.state)
        self.assertEqual(saved, self.data)

    def test_conflict_prevents_publication(self):
        snapshot = json.loads(self.state.read_bytes())
        snapshot['etag'] = '"superseded"'
        self.state.write_bytes(store.encode(snapshot))
        with self.assertRaisesRegex(ValueError, 'newer generation'):
            store.publish(self.checkout, self.state)
        self.assertNotIn('/state',self.data)

class TransportRetryTests(unittest.TestCase):
    def test_committed_put_with_lost_response_is_verified(self):
        import hashlib, os
        from unittest.mock import patch, Mock
        import requests
        from scripts import earn_recovery_store as transport
        body=b'content'
        responses=[requests.Timeout(), Mock(status_code=412), Mock(status_code=200,content=body)]
        with patch.dict(os.environ,{'EARN_STORAGE_KEY':'fixture'}), patch.object(transport.time,'sleep'), \
             patch.object(transport.requests,'request',side_effect=responses) as network:
            transport.conditional_put('/state',body,None)
            self.assertEqual([x.args[0] for x in network.call_args_list],['PUT','PUT','GET'])

    def test_unauthorized_request_fails_immediately(self):
        import os
        from unittest.mock import patch,Mock
        from scripts import earn_recovery_store as transport
        with patch.dict(os.environ,{'EARN_STORAGE_KEY':'fixture'}),patch.object(transport.requests,'request',return_value=Mock(status_code=403)) as network:
            with self.assertRaisesRegex(RuntimeError,'HTTP 403'): transport.request('GET','/state')
            self.assertEqual(network.call_count,1)
