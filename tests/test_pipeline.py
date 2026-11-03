import json
import pytest
from lakehouse.generate import generate, fixtures
from lakehouse.pipeline import Pipeline, local_spark

@pytest.fixture(scope='session')
def spark():
    s = local_spark()
    yield s
    s.stop()

def test_generator(tmp_path):
    generate(tmp_path)
    first = (tmp_path / 'batch_01.jsonl').read_bytes()
    generate(tmp_path)
    assert (tmp_path / 'batch_01.jsonl').read_bytes() == first
    assert len(first.splitlines()) == 5

def test_delta_end_to_end(spark, tmp_path):
    generate(tmp_path / 'input')
    p = Pipeline(spark, str(tmp_path / 'lake'))
    p.ingest(str(tmp_path / 'input/batch_01.jsonl'), 'b1')
    assert p.table('bronze').count() == 4
    assert p.table('quarantine').count() == 2
    assert p.reconcile() == {'matches': True, 'settled_transactions': 2, 'amount_minor': '12500'}
    p.ingest(str(tmp_path / 'input/batch_02.jsonl'), 'b2')
    assert p.table('silver_events').count() == 5
    assert p.table('silver_current').count() == 3
    assert p.reconcile() == {'matches': True, 'settled_transactions': 2, 'amount_minor': '15000'}
    rows = {str(r.settlement_date): int(r.amount_minor) for r in p.table('gold_settlement').collect()}
    assert rows == {'2026-01-01': 3000, '2026-01-02': 12000}
    before = p.table('bronze').count()
    p.ingest(str(tmp_path / 'input/batch_02.jsonl'), 'b2')
    assert p.table('bronze').count() == before
    assert p.reconcile()['amount_minor'] == '15000'
    # Late stale version cannot roll back current state, even in a new batch.
    p.ingest(str(tmp_path / 'input/batch_01.jsonl'), 'replay')
    assert p.reconcile()['amount_minor'] == '15000'

def test_invalid_conflicts_and_ties(spark, tmp_path):
    p = Pipeline(spark, str(tmp_path / 'lake'))
    base = fixtures()[0][0]
    records = [base, base | {'amount_minor': '99'},
               base | {'event_id': 'tie_a', 'transaction_id': 'tie', 'amount_minor': '5'},
               base | {'event_id': 'tie_z', 'transaction_id': 'tie', 'amount_minor': '7'},
               base | {'event_id': 'overflow', 'amount_minor': '999999999999999999999'},
               base | {'event_id': 'missing', 'account_id': None},
               base | {'event_id': 'date', 'settlement_date': '2026-02-30'},
               base | {'event_id': 'status', 'status': 'PENDING'},
               base | {'event_id': 'version', 'version': '0'},
               base | {'event_id': 'time', 'event_time': 'not-a-time'}]
    source = tmp_path / 'bad.jsonl'
    source.write_text(''.join(json.dumps(r) + '\n' for r in records) + '{broken\n')
    p.ingest(str(source), 'exceptions')
    reasons = [reason for row in p.table('quarantine').select('reasons').collect() for reason in row.reasons]
    for reason in ['event_id_conflict', 'invalid_amount', 'missing_account_id',
                   'invalid_settlement_date', 'invalid_status', 'invalid_version',
                   'invalid_event_time', 'malformed_json']:
        assert reason in reasons
    assert p.reconcile() == {'matches': True, 'settled_transactions': 1, 'amount_minor': '7'}
