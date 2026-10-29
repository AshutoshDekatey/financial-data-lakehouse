# Milestone 1: Databricks execution evidence

**Date:** 2026-10-28
**Source:** User-reported notebook output from Databricks Free Edition.
**Notebook:** `notebooks/01_foundation_pipeline.py`

```text
Processed batch_01
Processed batch_02
First pass: {'bronze': 14, 'quarantine': 6, 'silver': 5, 'gold': 3, 'settled_total': Decimal('450.00')}
Processed batch_01
Processed batch_02
After replay: {'bronze': 14, 'quarantine': 6, 'silver': 5, 'gold': 3, 'settled_total': Decimal('450.00')}
PASS: corrected amount, late event ordering, quarantine, and replay stability
```

User also reported that Databricks displayed the Silver, Gold, and Quarantine tables. This evidence demonstrates the assertions completed successfully on the user's Databricks workspace. Notebook screenshots and job-run links have **not** yet been added to the repository.

## Verified checks
- 14 unique raw event IDs in Bronze.
- 6 invalid events in Quarantine.
- 5 current payment states in Silver.
- 3 merchant-day groups in Gold and INR 450.00 settled total.
- No count or total change on replay.
- Corrected P001=120.00; late older P004 version does not replace 200.00.

This is a fixture-backed notebook, not yet source-file ingestion, orchestration, or recovery testing.
