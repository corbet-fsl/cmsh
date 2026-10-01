#!/usr/bin/env python3
"""Enforce immutable declarations and one locked revision per first-party crate."""
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

FIRST_PARTY = re.compile(r"^(?:git\+)?https://github\.com/corbet-(?:foss|libs)/")
SHA = re.compile(r"^[0-9a-f]{40}$")


def check_lock(packages):
    names = {p['name'] for p in packages if FIRST_PARTY.match(p.get('source', ''))}
    for name in names:
        entries = [p for p in packages if p['name'] == name]
        if len(entries) != 1:
            raise ValueError(f'Duplicate first-party crate: {name}')
        source = urlsplit(entries[0]['source'].removeprefix('git+'))
        query = parse_qs(source.query)
        rev = query.get('rev', [''])[0]
        if set(query) != {'rev'} or len(query['rev']) != 1 or not SHA.fullmatch(rev) or source.fragment != rev:
            raise ValueError(f'Expected one full matching revision for {name}')


def check_manifest(value):
    if isinstance(value, dict):
        if FIRST_PARTY.match(value.get('git', '')):
            if not SHA.fullmatch(value.get('rev', '')) or 'branch' in value or 'tag' in value:
                raise ValueError('First-party dependency must declare a full revision')
        for item in value.values():
            check_manifest(item)
    elif isinstance(value, list):
        for item in value:
            check_manifest(item)


def self_test():
    source = 'git+https://github.com/corbet-foss/example?rev=' + 'a' * 40 + '#' + 'a' * 40
    package = {'name': 'example', 'source': source}
    check_lock([package])
    check_lock([{'name': 'local'}])
    invalid = [[package, package], [package, {'name': 'example'}],
               [{'name': 'example', 'source': source.replace('?rev=', '?branch=')}],
               [{'name': 'example', 'source': source[:-1] + 'b'}]]
    for packages in invalid:
        try:
            check_lock(packages)
        except ValueError:
            continue
        raise AssertionError('Invalid lockfile accepted')
    for value in [{'git': 'https://github.com/corbet-libs/example', 'branch': 'main'},
                  {'git': 'https://github.com/corbet-foss/example', 'rev': 'abc123'}]:
        try:
            check_manifest({'dependencies': {'example': value}})
        except ValueError:
            continue
        raise AssertionError('Floating declaration accepted')


if __name__ == '__main__':
    self_test()
    if '--self-test' not in sys.argv:
        check_lock(tomllib.loads(Path('Cargo.lock').read_text())['package'])
        for manifest in Path('.').rglob('Cargo.toml'):
            if not {'.git', 'target', 'node_modules'}.intersection(manifest.parts):
                check_manifest(tomllib.loads(manifest.read_text()))
        print('First-party declarations and lockfile revisions are unique and pinned')
