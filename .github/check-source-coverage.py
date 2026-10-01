#!/usr/bin/env python3
"""Require all emitted source lines and branches from the same LLVM execution."""
import json
import re
from collections import Counter
import sys
from pathlib import Path


def check(lcov, raw_json, root, annotated):
    root = Path(root).resolve()

    def source_path(value):
        path = Path(value).resolve().relative_to(root)
        if path.suffix != '.rs' or 'tests' in path.parts or 'target' in path.parts:
            raise ValueError(f'Unexpected production source: {path}')
        return str(path)

    if raw_json.get('type') != 'llvm.coverage.json.export' or not raw_json.get('data'):
        raise ValueError('Missing companion raw LLVM JSON')
    expected = {}
    for unit in raw_json['data']:
        for item in unit['files']:
            path = source_path(item['filename'])
            if path in expected:
                raise ValueError('Duplicate raw source')
            expected[path] = item
    if not expected:
        raise ValueError('Empty production file inventory')
    files, lines, branches = set(), {}, {}
    current = None
    summaries = {}

    def finish():
        nonlocal current
        if current is None:
            raise ValueError('Unexpected end of record')
        local_branches = [count for (path, _, _, _), count in branches.items() if path == current]
        # LLVM's LF/LH and BRF/BRH summaries can count generic copies that
        # its merged DA/BRDA records do not. Check those summaries against the
        # companion export, but gate every emitted source counter below.
        summary = expected[current]['summary']
        locations = {tuple(branch[:4]) for branch in expected[current]['branches']}
        expected_counts = Counter(location[0] for location in locations)
        actual_counts = Counter(line for (path, line, _, _) in branches if path == current)
        if actual_counts != {line: 2 * count for line, count in expected_counts.items()}:
            raise ValueError('Missing emitted branch locations')
        for key, metric, field in [('LF', 'lines', 'count'), ('LH', 'lines', 'covered'),
                                  ('BRF', 'branches', 'count'), ('BRH', 'branches', 'covered')]:
            count = summary[metric][field]
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValueError('Invalid raw JSON summary')
            if key.startswith('BR') and not local_branches and count == 0 and key not in summaries:
                continue
            if summaries.get(key) != count:
                raise ValueError(f'Inconsistent {key} for {current}')
        current = None

    for record in lcov.splitlines():
        if not record:
            continue
        if record.startswith('SF:'):
            if current is not None:
                raise ValueError('Unterminated source record')
            current = source_path(record[3:])
            if current in files:
                raise ValueError('Duplicate source record')
            files.add(current)
            summaries = {}
        elif record == 'end_of_record':
            finish()
        elif record.startswith('DA:'):
            number, count, *_ = record[3:].split(',')
            key = (current, int(number))
            if current is None or int(number) <= 0 or key in lines or int(count) < 0:
                raise ValueError('Invalid or duplicate line record')
            lines[key] = int(count)
        elif record.startswith('BRDA:'):
            number, block, branch, count = record[5:].split(',')
            key = (current, int(number), int(block), int(branch))
            if current is None or min(key[1:]) < 0 or key[1] == 0 or key in branches:
                raise ValueError('Invalid or duplicate branch record')
            value = 0 if count == '-' else int(count)
            if value < 0:
                raise ValueError('Negative branch count')
            branches[key] = value
        elif record.split(':', 1)[0] in ('LF', 'LH', 'BRF', 'BRH'):
            key, value = record.split(':', 1)
            if current is None or key in summaries or int(value) < 0:
                raise ValueError('Invalid or duplicate summary')
            summaries[key] = int(value)
        elif not record.startswith(('TN:', 'FN:', 'FNDA:', 'FNF:', 'FNH:')):
            raise ValueError(f'Unexpected LCOV record: {record}')
    if current is not None or files != set(expected) or not lines:
        raise ValueError('Incomplete or empty source coverage inventory')
    # Cross-check every emitted line location against the same execution's
    # upstream annotated report. Summary counts alone can include generic copies
    # and therefore cannot detect a deleted DA record reliably.
    annotated_lines, annotated_files = {}, set()
    current = None
    # Upstream omits the filename heading for a single source file. Only that
    # unambiguous inventory may bind a heading-free annotated report.
    has_heading = any(row.endswith('.rs:') and row.startswith('/') for row in annotated.splitlines())
    if len(files) == 1 and not has_heading:
        current = next(iter(files))
        annotated_files.add(current)
    for record in annotated.splitlines():
        if record.endswith('.rs:') and record.startswith('/'):
            current = source_path(record[:-1])
            if current in annotated_files:
                raise ValueError('Duplicate annotated source')
            annotated_files.add(current)
        match = re.match(r'^\s*(\d+)\|\s*([0-9]+(?:\.[0-9]+)?[kMGT]?)\|', record)
        if match:
            key = (current, int(match[1]))
            if current is None or key in annotated_lines:
                raise ValueError('Invalid annotated line inventory')
            annotated_lines[key] = float(match[2].rstrip('kMGT')) > 0
    if annotated_files != files or set(annotated_lines) != set(lines):
        raise ValueError('Incomplete emitted line inventory')
    if any((lines[key] > 0) != hit for key, hit in annotated_lines.items()):
        raise ValueError('Inconsistent emitted line coverage')
    missing_lines = [key for key, count in lines.items() if count == 0]
    missing_branches = [key for key, count in branches.items() if count == 0]
    print(f'lines: {len(lines) - len(missing_lines)}/{len(lines)}')
    print(f'branches: {len(branches) - len(missing_branches)}/{len(branches)}')
    if missing_lines or missing_branches:
        raise ValueError(f'Uncovered source lines: {missing_lines}; branches: {missing_branches}')


if __name__ == '__main__':
    try:
        check(Path(sys.argv[1]).read_text(), json.loads(Path(sys.argv[2]).read_text()), Path.cwd(), Path(sys.argv[3]).read_text())
    except (ValueError, KeyError, TypeError, OSError) as error:
        sys.exit(f'Coverage gate: {error}')
