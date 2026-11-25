from copy import deepcopy
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recovery_borrow_feeds import CHAINS, sha256, validate

class BorrowGenerationTests(unittest.TestCase):
    def test_refuses_partial_or_mixed_generations(self):
        risk = {'positions':[{}], 'staleChains':[], 'chainStats':dict.fromkeys(CHAINS,{}), 'generatedAt':1}
        history = {'generatedAt':1, 'liquidationHistory':[{}]}
        earnings = {'sources':{'positionsPayloadSha256':sha256(risk)}, 'positions':[{}],
                    'chainStatus':{c:{'status':'ok'} for c in CHAINS}}
        validate(risk,history,earnings)
        for i in range(3):
            r,h,e = deepcopy((risk,history,earnings))
            if i == 0: r['staleChains'] = ['arbitrum']
            if i == 1: h['generatedAt'] += 1
            if i == 2: e['chainStatus'].pop('ethereum')
            with self.assertRaises(ValueError):
                validate(r,h,e)
