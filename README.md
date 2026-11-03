# Financial Data Lakehouse — v0.1

Independent Data Engineering portfolio project for financial-services payment processing;
not an actual JPMC system. Python, PySpark, Spark SQL, Databricks and Delta Lake.
AI-assisted implementation with executable verification; no real customer data.

`JSONL batches → Bronze raw Delta → Silver valid events/current state + quarantine → Gold settlement Delta`

Two deterministic batches demonstrate duplicates, corrections, voids, late arrivals and
invalid input. Amounts use integer minor units; currencies are never summed together.
See [architecture](docs/architecture.md), [version notes](docs/versions/v0.1.md), and
[execution evidence](docs/evidence.md).

## Local execution

Requires Python 3.10+, Java 17+ and network access to PyPI/Maven Central.
From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test]'
python -m lakehouse.generate data
python -m pytest -q
python -m lakehouse.demo
```

On environments where Java cannot use the inherited HTTPS proxy, use these additional
commands before pytest/demo (Python retains proxy and TLS verification). Maven Central
publishes SHA1 sidecars for these artifacts; the helper verifies them over HTTPS.

```bash
python scripts/fetch_jars.py
export LAKEHOUSE_JARS="$PWD/.cache/jars/delta-spark_2.13-4.0.0.jar,$PWD/.cache/jars/delta-storage-4.0.0.jar"
python -m pytest -q
python -m lakehouse.demo
```

The demo writes ignored `artifacts/demo` Delta tables. Repeating it is safe for the same
fixture version. Use a fresh output path when changing fixtures.

## Databricks execution

1. Use an **existing classic Databricks Runtime 17.3 LTS** cluster with Spark/Delta
   included. Do not install the local pinned PySpark/Delta packages on the cluster.
   The successful user-reported run did not specify its runtime version.
2. Add this repository as a Databricks Git folder. Open
   `notebooks/financial_lakehouse.py` as a Python source notebook and attach the cluster.
3. Have WRITE/READ VOLUME and USE CATALOG/SCHEMA permissions on an existing Unity
   Catalog volume. Enter a fresh path such as
   `/Volumes/<catalog>/<schema>/<volume>/financial_v01` in `output_root`.
   No resources are provisioned by this notebook.
4. Run all cells. If notebook working directory differs, set `repo` in the first cell
   to the absolute Git folder path. It must contain `src/lakehouse`.
5. Assertions must report totals **12500 → 15000 → 15000**, all reconciliations true,
   and two quarantined records. Inspect both displayed tables.
6. Run all cells again to verify rerun behavior. Tables persist; processes need not.

Verification: the project owner successfully executed the notebook in Databricks
and reported matching reconciliation totals **12500 → 15000 → 15000**, with two
settled transactions at every step. Gold settlement and quarantine tables were
printed. This confirms the supplied scenario and its in-notebook batch rerun.
See [evidence and provenance](docs/evidence.md) for the exact output.

The three automated tests were executed locally; the full exceptional-case suite,
concurrent writers, scale performance, cross-table failure recovery and a second
complete notebook execution remain unverified on Databricks. The remote runtime
version was not supplied. No production deployment or benchmark is claimed.
This is batch processing, not streaming.

## Expected final settlement

| Date | Account | Currency | Transactions | Amount (minor units) |
|---|---|---|---:|---:|
| 2026-01-01 | acct_001 | USD | 1 | 3000 |
| 2026-01-02 | acct_001 | USD | 1 | 12000 |

See limitations in the architecture document before adapting this to production.
