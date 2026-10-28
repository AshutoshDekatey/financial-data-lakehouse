# Financial Data Lakehouse

**Portfolio project (under active construction)**. A reproducible Databricks Free Edition implementation of a financial event lakehouse using PySpark, Delta Lake MERGE, Spark SQL, quality validation, and Bronze/Silver/Gold architecture. All data is synthetic.

## Milestone status
- [x] M1 starter notebook and deterministic fixture designed; **Databricks run pending**
- [ ] M1 confirmed with actual Databricks run + screenshots
- [ ] M2 file-based ingestion, incremental batch handling and edge-case tests
- [ ] M3 reconciliation, recovery and audit metrics
- [ ] M4 Databricks Jobs orchestration, evidence and polish

## Quickstart (Databricks Free Edition)
1. Create a free workspace from https://docs.databricks.com/aws/en/getting-started/free-edition . Use **Free Edition**, not the time-limited free trial.
2. In the workspace sidebar choose **Workspace**. Import `notebooks/01_foundation_pipeline.py` (as a notebook) into your user folder, or open from a Databricks Git folder.
3. Attach **Serverless** compute (if not automatically selected), then **Run all**. The notebook creates a `financial_lakehouse` schema inside `current_catalog()`.
4. Inspect Silver, Gold and Quarantine result tables in the final notebook cell.
5. Run the entire notebook again to verify replay stability.
6. If permissions prevent `CREATE SCHEMA`, create a schema through Catalog Explorer that you have rights to, then update `SCHEMA` in the first code cell.

## Expected results (not yet verified on Databricks)
| Dataset | Row count | Details |
|---|---:|---|
| Bronze | 14 | All distinct source event IDs |
| Quarantine | 6 | Missing amount/merchant, negative/non-numeric amount, bad status/date |
| Silver | 5 | 4 currently settled, 1 currently failed |
| Gold | 3 | Merchant-day groups; total settled INR 450.00 |

P001 changes from ₹100 to ₹120, while the late-arriving older P004 version **must not** replace ₹200 with ₹175. P002's duplicate business snapshot must not be double counted.

## Repository map
- `notebooks/01_foundation_pipeline.py`: importable Databricks source notebook (M1)
- `data/batch_01.csv`, `data/batch_02.csv`: illustrative source files for M2 ingestion; currently fixture equivalent
- `sql/inspect_results.sql`: optional human queries (assumes `workspace` catalog; adapt if needed)
- `docs/architecture.md`: architecture and business rules

## Data and behavior contracts
One event = one payment-state snapshot, with `event_version`. Raw events can be re-sent. Different event IDs can refer to the same payment. Silver chooses the highest version, or the latest event timestamp for a version tie. Gold reports latest payment states, not all historical payment events. Monetary data uses `DECIMAL(18,2)`.

## Scope / limitations
M1 uses embedded fixtures rather than file-based ingestion, and Gold is rebuilt in full. This is **not** a live banking integration, production deployment, streaming pipeline, reconciliation engine or scheduled Databricks Job yet. Those claims will be added only when implemented and validated.

## Running cost
Databricks Free Edition has serverless quota limits. No cloud billing account is required for this prototype.
