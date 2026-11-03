# v0.1 execution evidence

Date: 2026-11-03. Execution: local Linux task workspace, Python 3.12.14,
OpenJDK 21.0.12.1, PySpark 4.0.1, delta-spark 4.0.0, pytest 8.4.2.
The automated tests below ran on real local Spark/Delta. The separate Databricks
execution reported by the project owner is recorded below.

## Automated tests

From the repository root, after editable installation and verified JAR download:

```bash
export LAKEHOUSE_JARS="$PWD/.cache/jars/delta-spark_2.13-4.0.0.jar,$PWD/.cache/jars/delta-storage-4.0.0.jar"
.venv/bin/python -m pytest -q
```

Result: **3 passed in 108.93 seconds**.

- `test_generator`: reproducible synthetic input, five physical rows in initial batch.
- `test_delta_end_to_end`: Bronze duplicate collapse, quarantine, batch progression,
  correction, void, historical date arrival, semantic deduplication across batches,
  idempotent same-batch rerun and stale-version replay. Gold rows asserted by date.
- `test_invalid_conflicts_and_ties`: conflicting immutable event IDs, deterministic
  version ties, malformed JSON, overflow amount, missing account, invalid date,
  status, version and timestamp; reconciliation after quarantine.

Initial attempt: generator passed; both Spark tests errored before execution because
Ivy could not resolve Maven hosts. Remediation: verified HTTPS retrieval from Maven
Central with its published SHA1 sidecars, then local JAR configuration. No TLS or
checksum verification was disabled. A subsequent run started before JAR retrieval
completed also failed because Delta classes were unavailable; only the final complete suite is reported as passing.

Additional checks: `python -m compileall -q src notebooks tests scripts`,
`pip check` (no broken requirements), and `git diff --check` passed.
The actual `scripts/fetch_jars.py` helper executed successfully. Regenerating both
batches to a separate directory and comparing with `cmp` passed byte-for-byte.

## Limits of evidence

The Databricks results below are supplied by the project owner; the assistant did
not execute or independently inspect that remote run. Its execution date, runtime
version, cluster configuration and run URL were not supplied. The documented
17.3 LTS runtime remains a recommended target, not a confirmed runtime for this run.
No benchmark, production deployment or additional automated test results are claimed.
Concurrency, performance at scale and failure recovery across table writes remain
unverified. The full exceptional-case suite has only been verified locally, not
on Databricks. A second complete notebook execution was not reported; the third
reconciliation result demonstrates the repeated batch within the reported run.
The example CSV remains independently asserted by the local integration test;
individual rows of the displayed Databricks tables were not supplied.
This report does not claim production banking readiness.

## Real Databricks execution — user-reported

The project owner confirmed that the v0.1 notebook successfully completed in
Databricks and printed the Gold settlement and quarantine tables. The actual
reconciliation output supplied was:

```json
[
  {"matches": true, "settled_transactions": 2, "amount_minor": "12500"},
  {"matches": true, "settled_transactions": 2, "amount_minor": "15000"},
  {"matches": true, "settled_transactions": 2, "amount_minor": "15000"}
]
```

These results confirm notebook execution and reconciliation for the supplied
two-batch scenario: 12500 after batch 1, 15000 after batch 2, and unchanged
15000 after the in-notebook batch-2 rerun, each with two settled transactions.
This is a real Databricks execution reported by the user, separate from the
three local automated tests above.

## Executed demo

`LAKEHOUSE_JARS` as above, then `.venv/bin/python -m lakehouse.demo` completed
with exit code 0. Reconciled totals were 12500, 15000, 15000; all three checks
returned true with two settled transactions. The displayed final rows match
`examples/settlement.csv`. Captured application output: `examples/demo.txt`.
