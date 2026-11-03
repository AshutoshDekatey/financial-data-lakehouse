# Architecture and contracts

Bronze reads text lines, preserving raw JSON, batch identity and SHA256 record key.
Delta MERGE inserts unseen (batch ID, exact line) combinations. Identical lines within
a batch collapse; replay in another batch is retained for lineage. Input batches are
immutable and batch IDs are caller supplied. Whitespace-only lines may be ignored by
Spark text ingestion; this is not a byte-for-byte file archive.

Silver parses an explicit string schema and uses tolerant casts. Required IDs,
positive integer versions, nonnegative BIGINT minor-unit amounts, supported currency
(USD/EUR/GBP), dates, timestamps and SETTLED/VOID status are validated. Rejections
retain raw JSON, batch identity and an array of reasons. Unknown JSON fields are
ignored in v0.1. Timestamps follow Spark's timestamp parser and session UTC timezone.

An event ID identifies an immutable semantic payload. If historical payloads disagree
for one event ID, all variants are quarantined, including formerly accepted variants.
Semantic duplicates collapse across batches. Current state is selected per transaction
by version descending, event time descending, then event ID descending. Producers
must increment version for corrections; ties resolve deterministically but are not
proof of business correctness. There is no watermark: arbitrarily old dates remain
eligible. VOID excludes a transaction from settlement; it is not a negative payment.

Gold uses Spark SQL to group current SETTLED transactions by settlement date, account
and currency. SUM uses DECIMAL(38,0), avoiding floating point and BIGINT sum overflow.
Correction of an account/date/currency moves the entire current transaction. Reconciliation
compares Gold total and count with settled current state; aggregate totals across currencies
are only a mechanical integrity check, not a monetary valuation.

Bronze ingestion is incremental. Silver and Gold rebuild from the full Bronze history.
Each Delta table write is atomic, but the multi-table pipeline is not a single transaction.
Run one writer at a time and consume results only after successful completion. A failed
refresh can expose mixed generations; rerun restores derived state. Empty input on an
initialized lake is supported by Spark's schema, but a missing source path is an error.
No automatic scheduling, concurrent-writer coordination, access controls, bank holidays,
FX, double-entry ledger, fraud model or real payment connector is included.
