# Milestone 2: CSV-backed incremental transaction ingestion

## Setup

Create a **managed Unity Catalog volume** inside the schema created by M1:

```python
catalog = spark.sql('SELECT current_catalog()').first()[0]
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`financial_lakehouse`.`landing_files`')
print(f'/Volumes/{catalog}/financial_lakehouse/landing_files')
```

In Catalog Explorer, open this volume and upload the original `batch_01.csv`, `batch_02.csv`, plus `batch_03.csv` from `data/`. Use the volume's **root**, not a subfolder. Databricks file upload docs: https://docs.databricks.com/aws/en/volumes/volume-files

Import `notebooks/02_file_ingestion.py` and run all on serverless compute. It creates its own four Delta tables, so verified M1 tables stay intact.

## File arrivals and expected output

| After | Bronze | Quarantine | Silver | Gold | INR settled total |
| --- | ---: | ---: | ---: | ---: | ---: |
| First two files (in a new M2 environment) | 14 | 6 | 5 | 3 | 450.00 |
| Third file | 20 | 7 | 6 | 6 | 550.00 |
| Replay of all three files | 20 | 7 | 6 | 6 | 550.00 |

If the M2 tables already contain batch 03 from a previous successful run, the intermediate state after the first two inputs will already include those existing rows. The final state and replay assertions should remain correct.

Batch 03 tests:
- `P002`: 50 → 55 through version 2, moving from Oct 20 to Oct 21.
- `P003`: FAILED → SETTLED, adding 70 to current settled total.
- `P012`: two business-identical records with different event IDs; counted once as a payment.
- `P013`: non-numeric amount goes to quarantine.
- `P004`: newer timestamp on **older version 1** must not replace version 2 amount 200.

## Failure behavior and known limitations
- Missing or empty file triggers a failure; corrupt CSV parsing is configured to FAILFAST.
- Reuse of `event_id` with a different source payload triggers failure instead of silently discarding a changed event.
- A run that fails after Bronze but before Silver can be reprocessed because each batch re-reads committed Bronze events.
- An interrupted multi-table operation is not atomically rolled back as a unit; full failure/recovery injection and reconciliation remain for M3.
- Gold is a full refresh from latest Silver states, not an incremental MERGE of aggregates.

**Status:** Code prepared; execution in user's Databricks workspace is pending. Do not claim M2 verified until the notebook prints PASS.
