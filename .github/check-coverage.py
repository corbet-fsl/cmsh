#!/usr/bin/env python3
"""Gate exact LLVM line and branch counts, without rounded percentages."""
import json
import sys
from pathlib import Path


def check(report):
    if report.get('type') != 'llvm.coverage.json.export' or not report.get('data'):
        raise ValueError('Expected a nonempty LLVM coverage export')
    failures = []
    lines = 0
    for unit in report['data']:
        totals = unit['totals']
        for metric in ('lines', 'branches'):
            value = totals[metric]
            count, covered = value['count'], value['covered']
            if not isinstance(count, int) or not isinstance(covered, int) or not 0 <= covered <= count:
                raise ValueError(f'Invalid {metric} counts')
            if metric == 'lines':
                lines += count
            print(f'{metric}: {covered}/{count}')
            if covered != count:
                failures.append(f'{metric}: {count - covered} uncovered')
    if lines == 0:
        raise ValueError('Empty line coverage cannot pass')
    if failures:
        raise ValueError('; '.join(failures))


if __name__ == '__main__':
    try:
        check(json.loads(Path(sys.argv[1]).read_text()))
    except (ValueError, KeyError, TypeError) as error:
        sys.exit(f'Coverage gate: {error}')
