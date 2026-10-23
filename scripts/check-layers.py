#!/usr/bin/env python3
"""Check immediate package dependencies against explicit architectural rules."""
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parent.parent
rules = json.loads((root / 'architecture/layers.json').read_text())
module = subprocess.check_output(['go', 'list', '-m'], cwd=root, text=True).strip()
raw = subprocess.check_output(['go', 'list', '-json', './...'], cwd=root, text=True)
decoder = json.JSONDecoder()
violations = []
while raw.strip():
    package, end = decoder.raw_decode(raw.lstrip())
    raw = raw.lstrip()[end:]
    name = package['ImportPath'].removeprefix(module + '/')
    if name.startswith(('cmd/', 'examples/')):
        continue  # Composition roots may wire all public layers.
    if name not in rules:
        violations.append(f'{name}: no layer rule (add one deliberately)')
        continue
    for dependency in package.get('Imports', []):
        if dependency.startswith(module + '/'):
            local = dependency.removeprefix(module + '/')
            if local not in rules[name]:
                violations.append(f'{name} -> {local}: forbidden dependency')
        elif '.' in dependency.split('/')[0] and not name.startswith('adapters/'):
            violations.append(f'{name} -> {dependency}: external dependency must sit behind an adapter')
if violations:
    raise SystemExit('\n'.join(violations))
print('PASS: explicit layer dependency rules')
