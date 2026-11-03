# v0.1 execution evidence

Date: 2026-11-03. Execution: local Linux task workspace, Python 3.12.14,
OpenJDK 21.0.12.1, PySpark 4.0.1, delta-spark 4.0.0, pytest 8.4.2.
This is real local Spark/Delta execution, not a mocked pipeline or Databricks run.

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

Databricks notebook assertions have **not** been executed remotely. No Databricks
credentials, paid service provisioning or external resource changes were used.
Concurrency, performance at scale and failure recovery across table writes are not
validated. Example CSV is the expected final output, independently asserted by the
local integration test. This report does not claim production banking readiness.

## Executed demo

`LAKEHOUSE_JARS` as above, then `.venv/bin/python -m lakehouse.demo` completed
with exit code 0. Reconciled totals were 12500, 15000, 15000; all three checks
returned true with two settled transactions. The displayed final rows match
`examples/settlement.csv`. Captured application output: `examples/demo.txt`.
