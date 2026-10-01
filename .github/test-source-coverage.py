"""Exercise the actual source gate against LLVM-format acceptance/refusal cases."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('gate', Path(__file__).parent / 'check-source-coverage.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
ROOT = Path('/tmp/transport-coverage-fixture')
RAW = {'type': 'llvm.coverage.json.export', 'data': [{'files': [{'filename': str(ROOT / 'src/lib.rs'), 'branches': [[1, 1, 1, 2, 2, 1, 0, 0, 4]], 'summary': {'lines': {'count': 2, 'covered': 2}, 'branches': {'count': 2, 'covered': 2}}}]}]}
TEXT = f'{ROOT}/src/lib.rs:\n 1| 3|source\n 2| 1|source\n'
LCOV = f'SF:{ROOT}/src/lib.rs\nDA:1,3\nDA:2,1\nLF:2\nLH:2\nBRDA:1,0,0,2\nBRDA:1,0,1,1\nBRF:2\nBRH:2\nend_of_record\n'


class GateTests(unittest.TestCase):
    def test_complete_source_coverage(self):
        gate.check(LCOV, RAW, ROOT, TEXT)

    def test_merged_generic_source_is_not_instantiation_coverage(self):
        raw = copy.deepcopy(RAW)
        file = raw['data'][0]['files'][0]
        file['branches'].append(file['branches'][0][:])
        file['summary']['lines'] = {'count': 3, 'covered': 2}
        file['summary']['branches'] = {'count': 4, 'covered': 3}
        report = LCOV.replace('LF:2', 'LF:3').replace('BRF:2', 'BRF:4').replace('BRH:2', 'BRH:3')
        gate.check(report, raw, ROOT, TEXT)

    def test_no_instrumentable_branches(self):
        value = '\n'.join(row for row in LCOV.splitlines() if not row.startswith('BR'))
        raw = copy.deepcopy(RAW)
        raw['data'][0]['files'][0]['summary']['branches'] = {'count': 0, 'covered': 0}
        raw['data'][0]['files'][0]['branches'] = []
        gate.check(value, raw, ROOT, TEXT)

    def test_refuses_missing_malformed_or_uncovered_records(self):
        bad = [
            '', LCOV.replace('DA:2,1\n', ''), LCOV.replace('DA:2,1', 'DA:2,0'),
            LCOV.replace('BRDA:1,0,1,1', 'BRDA:1,0,1,0'),
            LCOV.replace('BRDA:1,0,1,1\n', ''), LCOV.replace('DA:2,1', 'DA:2,0').replace('LH:2', 'LH:1'),
            LCOV.replace('BRDA:1,0,1,1', 'BRDA:1,0,1,0').replace('BRH:2', 'BRH:1'),
            LCOV.replace('BRDA:1,0,1,1', 'BRDA:1,0,1,-').replace('BRH:2', 'BRH:1'),
            LCOV.replace('DA:1,3', 'DA:1,-1'),
            LCOV.replace('BRDA:1,0,0,2', 'BRDA:1,0,0,-2'),
            LCOV.replace('DA:1,3', 'DA:1,3\nDA:1,3'),
            LCOV.replace('BRDA:1,0,0,2', 'BRDA:1,0,0,2\nBRDA:1,0,0,2'),
            LCOV.replace('LF:2', 'LF:3'), LCOV.replace('BRH:2', 'BRH:1'),
            LCOV.replace('LF:2', 'LF:2\nLF:2'),
            LCOV.replace('end_of_record\n', ''), LCOV + LCOV,
            LCOV.replace('/src/lib.rs', '/tests/lib.rs'),
            LCOV.replace('/src/lib.rs', '/../outside.rs'),
            LCOV.replace('DA:1,3', 'DA:0,3'), LCOV + 'DA:4,1\n',
            LCOV.replace('LF:2\n', ''), LCOV.replace('BRF:2\n', ''),
        ]
        for index, report in enumerate(bad):
            with self.subTest(index=index), self.assertRaises(ValueError):
                gate.check(report, RAW, ROOT, TEXT)

    def test_companion_inventory_cannot_be_missing_or_truncated(self):
        raw = copy.deepcopy(RAW)
        raw['data'][0]['files'].append({'filename': str(ROOT / 'src/missing.rs'), 'branches': [], 'summary': copy.deepcopy(RAW['data'][0]['files'][0]['summary'])})
        for report in ({}, {'type': 'other', 'data': []}, raw):
            with self.subTest(report=report), self.assertRaises(ValueError):
                gate.check(LCOV, report, ROOT, TEXT)


    def test_upstream_single_file_report_omits_heading(self):
        gate.check(LCOV, RAW, ROOT, TEXT.split('\n', 1)[1])

    def test_duplicate_raw_source_refused(self):
        raw = copy.deepcopy(RAW)
        raw['data'][0]['files'].append(copy.deepcopy(raw['data'][0]['files'][0]))
        with self.assertRaises(ValueError):
            gate.check(LCOV, raw, ROOT, TEXT)

    def test_annotated_inventory_and_hit_consistency(self):
        for text in ['', TEXT + TEXT, TEXT.replace(' 2| 1|source\n', ''), TEXT.replace('2| 1|', '2| 0|')]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                gate.check(LCOV, RAW, ROOT, text)


if __name__ == '__main__':
    unittest.main()
