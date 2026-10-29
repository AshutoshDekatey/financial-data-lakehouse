# Financial Data Lakehouse

**GitHub portfolio project (under active construction).** Databricks Free Edition + PySpark + Delta Lake + Spark SQL. Synthetic financial payment-state events demonstrate schema validation, corrections, late events, current-state MERGE, quarantine, replay safety, and business-level aggregates.

## Milestone status
- [x] **M1:** Embedded-fixture Bronze → Silver → Gold pipeline. Executed and passed in Databricks on 2026-10-28 (see [`docs/milestone_1_evidence.md`](docs/milestone_1_evidence.md)).
- [ ] **M2:** CSV-backed ingestion via a Unity Catalog volume; incremental third batch and replay checks. Implementation added, **Databricks run pending**.
- [ ] **M3:** Reconciliation, more edge cases, interrupted-run recovery and audit metrics.
- [ ] **M4:** Databricks Jobs orchestration, final evidence and documentation polish.

## Quickstart

### M1: Foundation (verified)
1. Import [`notebooks/01_foundation_pipeline.py`](notebooks/01_foundation_pipeline.py) into Databricks.
2. Run all cells on serverless compute.
3. Expected: Bronze 14; Quarantine 6; Silver 5; Gold 3; settled INR 450.00. Rerun must not change the result.

### M2: Read real CSV files (awaiting Databricks verification)
1. Run this in an M1 notebook Python cell:
   ```python
   catalog = spark.sql('SELECT current_catalog()').first()[0]
   spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`financial_lakehouse`.`landing_files`')
   print(f'/Volumes/{catalog}/financial_lakehouse/landing_files')
   ```
2. In **Catalog Explorer**, navigate to `financial_lakehouse > landing_files` and upload `data/batch_01.csv`, `data/batch_02.csv`, and `data/batch_03.csv` from this repository into its root. See [official Databricks volume upload guide](https://docs.databricks.com/aws/en/volumes/volume-files).
3. Import [`notebooks/02_file_ingestion.py`](notebooks/02_file_ingestion.py) and **Run all** on serverless compute.
4. Expected after all three: Bronze 20; Quarantine 7; Silver 6; Gold 6; settled INR 550.00. Replay must keep those values unchanged.
5. M2 outputs live in separate tables named `bronze_file_transactions`, `silver_file_payments`, `quarantine_file_events`, `gold_file_daily_payments`, leaving M1 tables untouched.

The notebook checks expected results with assertions and prints an explicit PASS on success. See [`docs/milestone_2_guide.md`](docs/milestone_2_guide.md) for details.

## Business contract
- **Event grain:** each unique event ID is a payment-state snapshot. Duplicate delivery of the same ID is not a new event.
- **Silver grain:** one latest snapshot per `payment_id`; higher `event_version` wins, then newer `event_ts` if the version is equal.
- **Gold grain:** one `(business_date, merchant_id)` aggregate of *current* payment states. Settled totals include only `SETTLED` payments; failures are counts of current `FAILED` states.
- Example: `P001` is corrected from ₹100 to ₹120. `P004` remains ₹200 even though its older ₹175 version arrives later. The third batch corrects `P002`, changes `P003` from failed to settled, and adds `P012`.
- Currency is modeled with `DECIMAL(18,2)` rather than binary floating-point math.

## Repository map
- `notebooks/01_foundation_pipeline.py`: self-contained M1 pipeline.
- `notebooks/02_file_ingestion.py`: M2 volume-backed ingestion and incremental event state.
- `data/batch_01.csv`, `data/batch_02.csv`, `data/batch_03.csv`: synthetic test files.
- `sql/inspect_results.sql`, `sql/inspect_file_results.sql`: example SQL queries (catalog may need adapting).
- `docs/architecture.md`: flow, grains, ordering, limitations.
- `docs/milestone_1_evidence.md`: verified M1 output from Databricks.
- `docs/milestone_2_guide.md`: M2 execution and expected results.

## Constraints and engineering trade-offs
This project is a **portfolio prototype, not a production deployment**. M2 requires manually uploaded CSV files. It does not claim automated file arrival, streaming, production access controls or end-to-end atomic transactions. Bronze and Silver use Delta MERGE. Gold is deliberately recalculated from Silver on each run to avoid stale historical group totals when a payment changes business date or status. Cross-batch conflicting records with identical version/timestamp are not yet resolved by a strict conflict policy. M3 will address reconciliation and failure recovery. Free Edition has serverless quotas.
