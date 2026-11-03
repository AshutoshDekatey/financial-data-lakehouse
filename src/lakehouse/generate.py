"""Deterministic, deliberately small fixtures; no external data or credentials."""
import json
from pathlib import Path

FIELDS = ['event_id', 'transaction_id', 'version', 'event_time', 'settlement_date',
          'account_id', 'currency', 'amount_minor', 'status']

def fixtures():
    def event(e, t, version='1', amount='10000', **kw):
        return dict(zip(FIELDS, [e, t, version, '2026-01-02T10:00:00Z',
                    '2026-01-02', 'acct_001', 'USD', amount, 'SETTLED'])) | kw
    a = event('e1', 't1')
    return [
        [a, a, event('e2', 't2', amount='2500'),
         event('bad1', 'bad', amount='-5'), event('bad2', 'bad2', currency='XYZ')],
        [event('e3', 't1', '2', '12000'),
         event('e4', 't3', amount='3000', settlement_date='2026-01-01'),
         event('e5', 't2', '2', '2500', status='VOID'), a],
    ]

def generate(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for i, rows in enumerate(fixtures(), 1):
        (directory / f'batch_{i:02}.jsonl').write_text(
            ''.join(json.dumps(r, sort_keys=True) + '\n' for r in rows))

if __name__ == '__main__':
    import sys
    generate(sys.argv[1] if len(sys.argv) > 1 else 'data')
