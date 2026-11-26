from datetime import datetime
import io
import json
from pathlib import Path
import sys
import unittest
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import recovery_revenue as publication


class RevenuePublicationTests(unittest.TestCase):
    def pack(self, edit):
        root=publication.ROOT
        docs={name:json.loads((root/name).read_bytes()) for name in publication.FILES}
        edit(docs)
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:
            for name,data in docs.items():
                archive.writestr(name,json.dumps(data))
        return stream.getvalue()

    def test_existing_generation_passes_and_holes_in_evidence_are_rejected(self):
        self.assertIsInstance(publication.verified(self.pack(lambda tables:None)),datetime)
        with self.assertRaisesRegex(ValueError,'onchain'):
            publication.verified(self.pack(lambda tables:tables[publication.AUDIT]['chains'].pop('Arbitrum')))
        with self.assertRaisesRegex(ValueError,'30-day'):
            publication.verified(self.pack(lambda tables:tables[publication.REVENUE]['series'].pop(-5)))
        with self.assertRaisesRegex(ValueError,'rebate'):
            publication.verified(self.pack(lambda tables:tables[publication.REBATES]['chains']['Berachain'].update(reconciled=False)))
